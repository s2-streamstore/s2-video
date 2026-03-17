package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/s2-streamstore/s2-sdk-go/s2"
)

var upgrader = websocket.Upgrader{
	ReadBufferSize:  64 * 1024,
	WriteBufferSize: 64 * 1024,
	CheckOrigin: func(r *http.Request) bool {
		origin := r.Header.Get("Origin")
		if origin == "" {
			return true
		}
		u, err := url.Parse(origin)
		if err != nil {
			return false
		}
		return u.Host == r.Host
	},
}

func validID(s string) bool {
	if len(s) == 0 || len(s) > 64 {
		return false
	}
	for _, c := range s {
		if !((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || c == '-' || c == '_') {
			return false
		}
	}
	return true
}

type Hub struct {
	basin    *s2.BasinClient
	logger   *slog.Logger
	registry *RoomRegistry

	rooms   map[string]*Room
	roomsMu sync.RWMutex

	ctx    context.Context
	cancel context.CancelFunc
}

func NewHub(basin *s2.BasinClient, logger *slog.Logger) *Hub {
	ctx, cancel := context.WithCancel(context.Background())
	return &Hub{
		basin:  basin,
		logger: logger,
		rooms:  make(map[string]*Room),
		ctx:    ctx,
		cancel: cancel,
	}
}

func (h *Hub) Shutdown() {
	h.cancel()
	h.roomsMu.RLock()
	rooms := make([]*Room, 0, len(h.rooms))
	for _, r := range h.rooms {
		rooms = append(rooms, r)
	}
	h.roomsMu.RUnlock()

	for _, r := range rooms {
		r.Close()
	}
}

func (h *Hub) getOrCreateRoom(roomID string) *Room {
	h.roomsMu.RLock()
	room, ok := h.rooms[roomID]
	h.roomsMu.RUnlock()
	if ok {
		return room
	}

	h.roomsMu.Lock()
	defer h.roomsMu.Unlock()

	if room, ok := h.rooms[roomID]; ok {
		return room
	}

	room = NewRoom(roomID, h.basin, h.logger, h.ctx)
	room.onEmpty = h.scheduleRoomCleanup
	h.rooms[roomID] = room
	return room
}

const roomCleanupGracePeriod = 5 * time.Minute

func (h *Hub) scheduleRoomCleanup(roomID string) {
	go func() {
		select {
		case <-time.After(roomCleanupGracePeriod):
		case <-h.ctx.Done():
			return
		}

		h.roomsMu.Lock()
		defer h.roomsMu.Unlock()

		room, exists := h.rooms[roomID]
		if !exists {
			return
		}

		room.participantsMu.RLock()
		empty := len(room.participants) == 0
		room.participantsMu.RUnlock()

		if empty {
			room.Close()
			delete(h.rooms, roomID)
			h.logger.Info("room cleaned up", "room", roomID)
		}
	}()
}

func (h *Hub) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	roomID := r.URL.Query().Get("room")
	userID := r.URL.Query().Get("user")
	if !validID(roomID) || !validID(userID) {
		http.Error(w, "invalid room or user", http.StatusBadRequest)
		return
	}

	replay := r.URL.Query().Get("replay") == "true"
	var replayFrom uint64
	if fromStr := r.URL.Query().Get("from"); fromStr != "" {
		var err error
		replayFrom, err = strconv.ParseUint(fromStr, 10, 64)
		if err != nil {
			http.Error(w, "invalid 'from' parameter", http.StatusBadRequest)
			return
		}
	}
	replaySpeed := 1.0
	if speedStr := r.URL.Query().Get("speed"); speedStr != "" {
		if s, err := strconv.ParseFloat(speedStr, 64); err == nil && s > 0 {
			replaySpeed = s
		}
	}
	replayFocus := r.URL.Query().Get("focus") // Optional: replay only this participant's media.

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("websocket upgrade failed", "error", err)
		return
	}

	h.logger.Info("websocket connected", "room", roomID, "user", userID, "replay", replay)

	room := h.getOrCreateRoom(roomID)

	if replay {
		h.handleReplay(conn, room, userID, replayFrom, replaySpeed, replayFocus)
		return
	}

	h.handleLive(conn, room, userID)
}

const (
	wsReadLimit    = 4 * 1024 * 1024 // 4 MB max frame
	wsReadTimeout  = 30 * time.Second
	maxMsgPerSec   = 150
	maxChatBytes   = 10 * 1024 // 10 KB
)

func (h *Hub) handleLive(conn *websocket.Conn, room *Room, userID string) {
	participant := room.Join(userID, conn)
	if participant == nil {
		conn.WriteMessage(websocket.CloseMessage,
			websocket.FormatCloseMessage(websocket.CloseInternalServerErr, "failed to join room"))
		conn.Close()
		return
	}

	defer func() {
		room.Leave(userID)
		conn.Close()
	}()

	conn.SetReadLimit(wsReadLimit)

	var rateCount int
	rateWindowStart := time.Now()

	for {
		conn.SetReadDeadline(time.Now().Add(wsReadTimeout))
		msgType, data, err := conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				h.logger.Error("ws read error", "user", userID, "error", err)
			}
			return
		}

		if msgType != websocket.BinaryMessage {
			continue
		}

		now := time.Now()
		if now.Sub(rateWindowStart) >= time.Second {
			rateCount = 0
			rateWindowStart = now
		}
		rateCount++
		if rateCount > maxMsgPerSec {
			h.logger.Warn("rate limit exceeded, closing connection", "user", userID)
			conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "rate limit exceeded"))
			return
		}

		typ, payload, err := DecodeClientMessage(data)
		if err != nil {
			h.logger.Error("decode error", "user", userID, "error", err)
			continue
		}

		switch typ {
		case MsgTypeVideo, MsgTypeAudio, MsgTypeScreen:
			participant.AppendMedia(typ, payload)
		case MsgTypeControl:
			room.AppendControl(userID, payload)
		case MsgTypeChat:
			if len(payload) > maxChatBytes {
				continue
			}
			room.AppendChat(userID, payload)
		}
	}
}

func (h *Hub) handleReplay(conn *websocket.Conn, room *Room, userID string, fromTimestamp uint64, speed float64, focus string) {
	defer conn.Close()

	allParticipants, err := room.GetParticipantHistory()
	if err != nil {
		h.logger.Error("failed to get participant history", "room", room.id, "error", err)
		return
	}
	sort.Strings(allParticipants)

	if len(allParticipants) == 0 {
		ctrl, _ := EncodeControlMessage(ControlMessage{
			Type:    ControlError,
			Message: "no participants found in this room",
		})
		conn.WriteMessage(websocket.BinaryMessage, ctrl)
		return
	}

	participants := allParticipants
	replayFocus := focus
	if replayFocus != "" {
		found := false
		for _, p := range allParticipants {
			if p == replayFocus {
				participants = []string{replayFocus}
				found = true
				break
			}
		}
		if !found {
			replayFocus = ""
		}
	}

	ctx, cancel := context.WithCancel(h.ctx)
	defer cancel()

	go func() {
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				cancel()
				return
			}
		}
	}()

	var wg sync.WaitGroup
	wsMu := &sync.Mutex{}

	startTs := fromTimestamp
	roomEnd := startTs
	if roomStart, computedEnd, err := h.computeReplayBounds(ctx, room, allParticipants); err == nil {
		roomEnd = computedEnd
		if startTs == 0 {
			startTs = roomStart
		} else {
			if startTs < roomStart {
				startTs = roomStart
			}
			if roomEnd > 0 && startTs > roomEnd {
				startTs = roomEnd
			}
		}
	}
	if startTs == 0 {
		mediaName := fmt.Sprintf("rooms/%s/media/%s", room.id, participants[0])
		mediaStream := h.basin.Stream(s2.StreamName(mediaName))
		if batch, err := mediaStream.Read(ctx, &s2.ReadOptions{
			SeqNum: s2.Uint64(0),
			Count:  s2.Uint64(1),
		}); err == nil && len(batch.Records) > 0 {
			startTs = batch.Records[0].Timestamp
		}
	}

	snapshotParticipants := participants
	if snapshot, err := h.buildReplaySnapshot(ctx, room, startTs, allParticipants, replayFocus); err == nil {
		snapshotParticipants = snapshot.ActiveParticipants
		ctrl, _ := EncodeControlMessage(ControlMessage{
			Type:         ControlParticipants,
			Participants: snapshot.ActiveParticipants,
			Timestamp:    int64(startTs),
		})
		if err := conn.WriteMessage(websocket.BinaryMessage, ctrl); err != nil {
			return
		}
		for _, msg := range snapshot.HandMessages {
			if err := conn.WriteMessage(websocket.BinaryMessage, msg); err != nil {
				return
			}
		}
		for _, msg := range snapshot.ChatMessages {
			if err := conn.WriteMessage(websocket.BinaryMessage, msg); err != nil {
				return
			}
		}
	} else {
		ctrl, _ := EncodeControlMessage(ControlMessage{
			Type:         ControlParticipants,
			Participants: snapshotParticipants,
			Timestamp:    int64(startTs),
		})
		if err := conn.WriteMessage(websocket.BinaryMessage, ctrl); err != nil {
			return
		}
	}

	if err := sendReplayProgress(conn, startTs); err != nil {
		return
	}

	clock := &replayClock{
		wallStart: time.Now(),
		originTs:  startTs,
		speed:     speed,
	}

	for _, pid := range participants {
		wg.Add(1)
		go func(pid string) {
			defer wg.Done()
			h.replayMediaStream(ctx, conn, wsMu, room, pid, startTs, speed, clock)
		}(pid)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		h.replayMetaStream(ctx, conn, wsMu, room, startTs, speed, clock, replayFocus)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		h.replayChatStream(ctx, conn, wsMu, room, startTs, speed, clock)
	}()

	wg.Wait()

	completeTs := roomEnd
	if completeTs < startTs {
		completeTs = startTs
	}
	ctrl, _ := EncodeControlMessage(ControlMessage{
		Type:      ControlReplayComplete,
		Timestamp: int64(completeTs),
	})
	wsMu.Lock()
	conn.WriteMessage(websocket.BinaryMessage, ctrl)
	wsMu.Unlock()
}

func (h *Hub) replayMediaStream(ctx context.Context, conn *websocket.Conn, wsMu *sync.Mutex, room *Room, participantID string, fromTimestamp uint64, speed float64, clock *replayClock) {
	streamName := fmt.Sprintf("rooms/%s/media/%s", room.id, participantID)
	stream := h.basin.Stream(s2.StreamName(streamName))

	readOpts := &s2.ReadOptions{
		Wait:  s2.Int32(0),
		Clamp: s2.Bool(true),
	}
	if fromTimestamp > 0 {
		readOpts.Timestamp = &fromTimestamp
	} else {
		readOpts.SeqNum = s2.Uint64(0)
	}

	session, err := stream.ReadSession(ctx, readOpts)
	if err != nil {
		h.logger.Error("replay media read session failed", "stream", streamName, "error", err)
		return
	}
	defer session.Close()

	for session.Next() {
		rec := session.Record()
		mediaType := mediaTypeFromHeaders(rec.Headers)
		if mediaType == 0 {
			continue
		}

		if !clock.waitUntil(ctx, rec.Timestamp) {
			return
		}

		msg := EncodeMediaMessage(mediaType, participantID, rec.Body)
		wsMu.Lock()
		err := conn.WriteMessage(websocket.BinaryMessage, msg)
		wsMu.Unlock()
		if err != nil {
			return
		}
	}

	if err := session.Err(); err != nil {
		h.logger.Error("replay media read error", "stream", streamName, "error", err)
	}
}

func (h *Hub) replayMetaStream(ctx context.Context, conn *websocket.Conn, wsMu *sync.Mutex, room *Room, fromTimestamp uint64, speed float64, clock *replayClock, focus string) {
	streamName := fmt.Sprintf("rooms/%s/meta", room.id)
	stream := h.basin.Stream(s2.StreamName(streamName))

	readOpts := &s2.ReadOptions{
		Wait:  s2.Int32(0),
		Clamp: s2.Bool(true),
	}
	if fromTimestamp > 0 {
		readOpts.Timestamp = &fromTimestamp
	} else {
		readOpts.SeqNum = s2.Uint64(0)
	}

	session, err := stream.ReadSession(ctx, readOpts)
	if err != nil {
		return
	}
	defer session.Close()

	for session.Next() {
		rec := session.Record()

		var event MetaEvent
		if err := json.Unmarshal(rec.Body, &event); err != nil {
			continue
		}
		if focus != "" && event.UserID != focus {
			continue
		}

		var msg []byte
		switch event.Type {
		case "join":
			msg, _ = EncodeControlMessage(ControlMessage{Type: ControlJoin, UserID: event.UserID, Timestamp: int64(rec.Timestamp)})
		case "leave":
			msg, _ = EncodeControlMessage(ControlMessage{Type: ControlLeave, UserID: event.UserID, Timestamp: int64(rec.Timestamp)})
		case "control":
			payload := augmentReplayControlPayload(event.Data, rec.Timestamp)
			if len(payload) == 0 {
				continue
			}
			msg = make([]byte, 1+len(payload))
			msg[0] = MsgTypeControl
			copy(msg[1:], payload)
		default:
			continue
		}

		if !clock.waitUntil(ctx, rec.Timestamp) {
			return
		}

		wsMu.Lock()
		err := conn.WriteMessage(websocket.BinaryMessage, msg)
		wsMu.Unlock()
		if err != nil {
			return
		}
	}
}

func (h *Hub) replayChatStream(ctx context.Context, conn *websocket.Conn, wsMu *sync.Mutex, room *Room, fromTimestamp uint64, speed float64, clock *replayClock) {
	streamName := fmt.Sprintf("rooms/%s/chat", room.id)
	stream := h.basin.Stream(s2.StreamName(streamName))

	readOpts := &s2.ReadOptions{Wait: s2.Int32(0)}
	if fromTimestamp > 0 {
		readOpts.Timestamp = &fromTimestamp
	} else {
		readOpts.SeqNum = s2.Uint64(0)
	}

	session, err := stream.ReadSession(ctx, readOpts)
	if err != nil {
		return
	}
	defer session.Close()

	for session.Next() {
		rec := session.Record()

		if !clock.waitUntil(ctx, rec.Timestamp) {
			return
		}

		msg := make([]byte, 1+len(rec.Body))
		msg[0] = MsgTypeChat
		copy(msg[1:], rec.Body)

		wsMu.Lock()
		err := conn.WriteMessage(websocket.BinaryMessage, msg)
		wsMu.Unlock()
		if err != nil {
			return
		}
	}
}

// replayClock synchronizes replay delivery to a shared timeline.
// Wall clock wallStart corresponds to S2 timestamp originTs.
// For a record at timestamp T, the wall clock target is:
//
//	wallStart + (T - originTs) / speed
type replayClock struct {
	wallStart time.Time
	originTs  uint64
	speed     float64
}

// waitUntil blocks until the wall clock reaches the point where recTs should be delivered.
// Returns false if the context is cancelled.
func (rc *replayClock) waitUntil(ctx context.Context, recTs uint64) bool {
	if recTs <= rc.originTs {
		return true
	}

	offsetMs := float64(recTs-rc.originTs) / rc.speed
	targetWall := rc.wallStart.Add(time.Duration(offsetMs) * time.Millisecond)
	delay := time.Until(targetWall)

	if delay <= 0 {
		return true
	}

	select {
	case <-time.After(delay):
		return true
	case <-ctx.Done():
		return false
	}
}

type MetaEvent struct {
	Type      string          `json:"type"`
	UserID    string          `json:"user_id"`
	Timestamp int64           `json:"timestamp"`
	Data      json.RawMessage `json:"data,omitempty"`
}

// GetParticipantHistory lists media streams to find all participants.
// Stream names are "rooms/{room}/media/{user}" — the stream's existence is the source of truth.
func (r *Room) GetParticipantHistory() ([]string, error) {
	prefix := fmt.Sprintf("rooms/%s/media/", r.id)
	iter := r.basin.Streams.Iter(r.ctx, &s2.ListStreamsArgs{
		Prefix: prefix,
	})

	var participants []string
	for iter.Next() {
		name := string(iter.Value().Name)
		// Extract user ID: strip the prefix.
		userID := name[len(prefix):]
		if userID != "" {
			participants = append(participants, userID)
		}
	}
	if err := iter.Err(); err != nil {
		return nil, err
	}

	return participants, nil
}
