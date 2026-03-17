package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"github.com/s2-streamstore/s2-sdk-go/s2"
)

const (
	participantSendBuffer  = 256
	reliableSendTimeout    = 1 * time.Second
	participantWriteLimit  = 5 * time.Second
	maxParticipantsPerRoom = 50
)

type Room struct {
	id     string
	basin  *s2.BasinClient
	logger *slog.Logger

	participants   map[string]*Participant
	participantsMu sync.RWMutex
	mediaReaders   map[string]*roomMediaReader
	mediaReadersMu sync.Mutex

	onEmpty           func(roomID string) // Called when last participant leaves.
	sharedReadersOnce sync.Once

	ctx    context.Context
	cancel context.CancelFunc
}

func NewRoom(id string, basin *s2.BasinClient, logger *slog.Logger, parentCtx context.Context) *Room {
	ctx, cancel := context.WithCancel(parentCtx)
	return &Room{
		id:           id,
		basin:        basin,
		logger:       logger,
		participants: make(map[string]*Participant),
		mediaReaders: make(map[string]*roomMediaReader),
		ctx:          ctx,
		cancel:       cancel,
	}
}

func (r *Room) Close() {
	r.cancel()
	r.participantsMu.RLock()
	for _, p := range r.participants {
		p.Close()
	}
	r.participantsMu.RUnlock()
}

func (r *Room) Join(userID string, conn *websocket.Conn) *Participant {
	r.participantsMu.Lock()

	if _, exists := r.participants[userID]; exists {
		r.participantsMu.Unlock()
		r.logger.Warn("user already in room", "user", userID, "room", r.id)
		return nil
	}

	if len(r.participants) >= maxParticipantsPerRoom {
		r.participantsMu.Unlock()
		r.logger.Warn("room full", "room", r.id)
		return nil
	}

	p, err := NewParticipant(userID, r, conn)
	if err != nil {
		r.participantsMu.Unlock()
		r.logger.Error("failed to create participant", "user", userID, "error", err)
		return nil
	}

	r.participants[userID] = p

	others := make([]*Participant, 0, len(r.participants))
	for otherID, other := range r.participants {
		if otherID == userID {
			continue
		}
		others = append(others, other)
	}

	r.participantsMu.Unlock()

	// These operations happen outside the lock to avoid deadlock.
	r.publishMetaEvent("join", userID)
	r.broadcastParticipantList()

	// Start shared room-level media readers and subscribe participants.
	inMemoryIDs := make(map[string]bool, len(others)+1)
	inMemoryIDs[userID] = true
	for _, other := range others {
		inMemoryIDs[other.userID] = true
		r.subscribeParticipantToMedia(p, other.userID)
		r.subscribeParticipantToMedia(other, userID)
	}

	// Also subscribe to participants on other instances discovered via S2 streams.
	// This handles the case where the room spans multiple backend instances.
	go func() {
		existing, err := r.GetParticipantHistory()
		if err != nil {
			return
		}
		for _, pid := range existing {
			if !inMemoryIDs[pid] {
				r.subscribeParticipantToMedia(p, pid)
			}
		}
	}()

	return p
}

func (r *Room) Leave(userID string) {
	r.participantsMu.Lock()
	p, exists := r.participants[userID]
	if !exists {
		r.participantsMu.Unlock()
		return
	}
	delete(r.participants, userID)
	empty := len(r.participants) == 0
	r.participantsMu.Unlock()

	r.removeParticipantFromMedia(userID)
	p.Close()

	r.publishMetaEvent("leave", userID)
	r.broadcastParticipantList()

	// Notify hub that this room might be empty.
	if empty && r.onEmpty != nil {
		r.onEmpty(r.id)
	}
}

func (r *Room) publishMetaEvent(eventType, userID string, data ...[]byte) {
	streamName := fmt.Sprintf("rooms/%s/meta", r.id)
	stream := r.basin.Stream(s2.StreamName(streamName))

	event := MetaEvent{
		Type:      eventType,
		UserID:    userID,
		Timestamp: time.Now().UnixMilli(),
	}
	if len(data) > 0 {
		event.Data = json.RawMessage(data[0])
	}
	body, _ := json.Marshal(event)

	ts := uint64(time.Now().UnixMilli())
	_, err := stream.Append(r.ctx, &s2.AppendInput{
		Records: []s2.AppendRecord{{
			Body:      body,
			Timestamp: &ts,
		}},
	})
	if err != nil {
		r.logger.Error("failed to publish meta event", "event", eventType, "user", userID, "error", err)
	}
}

func (r *Room) broadcastParticipantList() {
	r.participantsMu.RLock()
	participants := make([]*Participant, 0, len(r.participants))
	ids := make([]string, 0, len(r.participants))
	for id, p := range r.participants {
		ids = append(ids, id)
		participants = append(participants, p)
	}
	r.participantsMu.RUnlock()

	msg, _ := EncodeControlMessage(ControlMessage{
		Type:         ControlParticipants,
		Participants: ids,
	})

	for _, p := range participants {
		p.SendMessage(msg)
	}
}

func (r *Room) AppendControl(senderID string, payload []byte) {
	r.publishMetaEvent("control", senderID, payload)
}

func (r *Room) AppendChat(senderID string, text []byte) {
	streamName := fmt.Sprintf("rooms/%s/chat", r.id)
	stream := r.basin.Stream(s2.StreamName(streamName))

	msg := ChatMessage{
		UserID:    senderID,
		Text:      string(text),
		Timestamp: time.Now().UnixMilli(),
	}
	body, _ := json.Marshal(msg)

	ts := uint64(time.Now().UnixMilli())
	_, err := stream.Append(r.ctx, &s2.AppendInput{
		Records: []s2.AppendRecord{{
			Body:      body,
			Timestamp: &ts,
		}},
	})
	if err != nil {
		r.logger.Error("failed to append chat", "user", senderID, "error", err)
	}
}

type ChatMessage struct {
	UserID    string `json:"user_id"`
	Text      string `json:"text"`
	Timestamp int64  `json:"timestamp"`
}

type roomMediaReader struct {
	room     *Room
	senderID string

	ctx    context.Context
	cancel context.CancelFunc

	subscribers   map[string]*Participant
	subscribersMu sync.RWMutex
}

type Participant struct {
	userID string
	room   *Room
	conn   *websocket.Conn

	mediaProducer *s2.Producer
	mediaBatcher  *s2.Batcher
	mediaSession  *s2.AppendSession

	sendCh chan []byte

	stats   ParticipantStats
	statsMu sync.Mutex

	ctx    context.Context
	cancel context.CancelFunc
}

type ParticipantStats struct {
	VideoRecords  uint64 `json:"video_records"`
	AudioRecords  uint64 `json:"audio_records"`
	VideoBytes    uint64 `json:"video_bytes"`
	AudioBytes    uint64 `json:"audio_bytes"`
	ScreenRecords uint64 `json:"screen_records"`
	ScreenBytes   uint64 `json:"screen_bytes"`
}

func NewParticipant(userID string, room *Room, conn *websocket.Conn) (*Participant, error) {
	ctx, cancel := context.WithCancel(room.ctx)

	mediaStreamName := fmt.Sprintf("rooms/%s/media/%s", room.id, userID)
	mediaStream := room.basin.Stream(s2.StreamName(mediaStreamName))
	mediaSession, err := mediaStream.AppendSession(ctx, nil)
	if err != nil {
		cancel()
		return nil, fmt.Errorf("open media append session: %w", err)
	}
	mediaBatcher := s2.NewBatcher(ctx, &s2.BatchingOptions{
		Linger: 5 * time.Millisecond,
	})
	mediaProducer := s2.NewProducer(ctx, mediaBatcher, mediaSession)

	p := &Participant{
		userID:        userID,
		room:          room,
		conn:          conn,
		mediaProducer: mediaProducer,
		mediaBatcher:  mediaBatcher,
		mediaSession:  mediaSession,
		sendCh:        make(chan []byte, participantSendBuffer),
		ctx:           ctx,
		cancel:        cancel,
	}

	go p.writePump()

	room.sharedReadersOnce.Do(func() {
		go room.sharedMetaReader()
		go room.sharedChatReader()
		go room.sharedStatsBroadcaster()
	})

	return p, nil
}

func newRoomMediaReader(room *Room, senderID string) *roomMediaReader {
	ctx, cancel := context.WithCancel(room.ctx)
	return &roomMediaReader{
		room:        room,
		senderID:    senderID,
		ctx:         ctx,
		cancel:      cancel,
		subscribers: make(map[string]*Participant),
	}
}

func (r *Room) subscribeParticipantToMedia(participant *Participant, senderID string) {
	if participant == nil || participant.userID == senderID {
		return
	}

	var reader *roomMediaReader
	startReader := false

	r.mediaReadersMu.Lock()
	reader = r.mediaReaders[senderID]
	if reader == nil {
		reader = newRoomMediaReader(r, senderID)
		r.mediaReaders[senderID] = reader
		startReader = true
	}
	reader.addSubscriber(participant)
	r.mediaReadersMu.Unlock()

	if startReader {
		go reader.run()
	}
}

func (r *Room) removeParticipantFromMedia(userID string) {
	for _, senderID := range r.snapshotMediaReaderIDs() {
		if senderID == userID {
			r.stopMediaReader(senderID)
			continue
		}
		r.unsubscribeParticipantFromMedia(userID, senderID)
	}
}

func (r *Room) unsubscribeParticipantFromMedia(userID, senderID string) {
	var reader *roomMediaReader
	stopReader := false

	r.mediaReadersMu.Lock()
	reader = r.mediaReaders[senderID]
	if reader != nil {
		reader.removeSubscriber(userID)
		if reader.subscriberCount() == 0 {
			delete(r.mediaReaders, senderID)
			stopReader = true
		}
	}
	r.mediaReadersMu.Unlock()

	if stopReader {
		reader.cancel()
	}
}

func (r *Room) stopMediaReader(senderID string) {
	r.mediaReadersMu.Lock()
	reader := r.mediaReaders[senderID]
	if reader != nil {
		delete(r.mediaReaders, senderID)
	}
	r.mediaReadersMu.Unlock()

	if reader != nil {
		reader.cancel()
	}
}

func (r *Room) snapshotMediaReaderIDs() []string {
	r.mediaReadersMu.Lock()
	defer r.mediaReadersMu.Unlock()

	ids := make([]string, 0, len(r.mediaReaders))
	for senderID := range r.mediaReaders {
		ids = append(ids, senderID)
	}

	return ids
}

func (rmr *roomMediaReader) addSubscriber(participant *Participant) {
	rmr.subscribersMu.Lock()
	defer rmr.subscribersMu.Unlock()
	rmr.subscribers[participant.userID] = participant
}

func (rmr *roomMediaReader) removeSubscriber(userID string) {
	rmr.subscribersMu.Lock()
	defer rmr.subscribersMu.Unlock()
	delete(rmr.subscribers, userID)
}

func (rmr *roomMediaReader) subscriberCount() int {
	rmr.subscribersMu.RLock()
	defer rmr.subscribersMu.RUnlock()
	return len(rmr.subscribers)
}

func (rmr *roomMediaReader) snapshotSubscribers() []*Participant {
	rmr.subscribersMu.RLock()
	defer rmr.subscribersMu.RUnlock()

	participants := make([]*Participant, 0, len(rmr.subscribers))
	for _, participant := range rmr.subscribers {
		participants = append(participants, participant)
	}

	return participants
}

func (rmr *roomMediaReader) run() {
	streamName := fmt.Sprintf("rooms/%s/media/%s", rmr.room.id, rmr.senderID)
	stream := rmr.room.basin.Stream(s2.StreamName(streamName))

	for {
		if rmr.ctx.Err() != nil {
			return
		}

		// Establish a read session, retrying until the stream exists.
		var session *s2.ReadSession
		for {
			select {
			case <-rmr.ctx.Done():
				return
			default:
			}

			var err error
			session, err = stream.ReadSession(rmr.ctx, &s2.ReadOptions{
				TailOffset: s2.Int64(0),
			})
			if err == nil {
				break
			}

			rmr.room.logger.Info("media read session not ready, retrying", "stream", streamName, "error", err)
			select {
			case <-rmr.ctx.Done():
				return
			case <-time.After(500 * time.Millisecond):
			}
		}

		for session.Next() {
			select {
			case <-rmr.ctx.Done():
				session.Close()
				return
			default:
			}

			rec := session.Record()
			mediaType := mediaTypeFromHeaders(rec.Headers)
			if mediaType == 0 {
				continue
			}

			subscribers := rmr.snapshotSubscribers()
			if len(subscribers) == 0 {
				continue
			}

			msg := EncodeMediaMessage(mediaType, rmr.senderID, rec.Body)
			for _, participant := range subscribers {
				participant.SendMessage(msg)
			}
		}

		if err := session.Err(); err != nil && rmr.ctx.Err() == nil {
			rmr.room.logger.Error("media read session error, restarting", "stream", streamName, "error", err)
		}
		session.Close()

		// Brief pause before restarting to avoid tight retry loops.
		select {
		case <-rmr.ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func (r *Room) broadcastToAll(msg []byte) {
	for _, p := range r.snapshotParticipants() {
		p.SendMessage(msg)
	}
}

func (r *Room) broadcastToAllExcept(msg []byte, excludeUserID string) {
	for _, p := range r.snapshotParticipantsExcept(excludeUserID) {
		p.SendMessage(msg)
	}
}

func (r *Room) snapshotParticipants() []*Participant {
	r.participantsMu.RLock()
	defer r.participantsMu.RUnlock()

	participants := make([]*Participant, 0, len(r.participants))
	for _, p := range r.participants {
		participants = append(participants, p)
	}

	return participants
}

func (r *Room) snapshotParticipantsExcept(excludeUserID string) []*Participant {
	r.participantsMu.RLock()
	defer r.participantsMu.RUnlock()

	participants := make([]*Participant, 0, len(r.participants))
	for uid, p := range r.participants {
		if uid != excludeUserID {
			participants = append(participants, p)
		}
	}

	return participants
}

func (r *Room) sharedMetaReader() {
	streamName := fmt.Sprintf("rooms/%s/meta", r.id)
	stream := r.basin.Stream(s2.StreamName(streamName))

	var session *s2.ReadSession
	for {
		select {
		case <-r.ctx.Done():
			return
		default:
		}
		var err error
		session, err = stream.ReadSession(r.ctx, &s2.ReadOptions{
			TailOffset: s2.Int64(0),
		})
		if err == nil {
			break
		}
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
	defer session.Close()

	for session.Next() {
		select {
		case <-r.ctx.Done():
			return
		default:
		}

		rec := session.Record()
		var event MetaEvent
		if err := json.Unmarshal(rec.Body, &event); err != nil {
			continue
		}

		switch event.Type {
		case "join":
			// A participant joined — possibly on a different instance.
			// Subscribe all our local participants to their media stream.
			for _, local := range r.snapshotParticipants() {
				r.subscribeParticipantToMedia(local, event.UserID)
			}
		case "leave":
			// Stop our local reader for this participant's stream.
			r.stopMediaReader(event.UserID)
		case "control":
			msg := make([]byte, 1+len(event.Data))
			msg[0] = MsgTypeControl
			copy(msg[1:], event.Data)
			r.broadcastToAllExcept(msg, event.UserID)
		}
	}
}

func (r *Room) sharedChatReader() {
	streamName := fmt.Sprintf("rooms/%s/chat", r.id)
	stream := r.basin.Stream(s2.StreamName(streamName))

	var session *s2.ReadSession
	for {
		select {
		case <-r.ctx.Done():
			return
		default:
		}
		var err error
		session, err = stream.ReadSession(r.ctx, &s2.ReadOptions{
			SeqNum: s2.Uint64(0),
		})
		if err == nil {
			break
		}
		select {
		case <-r.ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
	defer session.Close()

	for session.Next() {
		select {
		case <-r.ctx.Done():
			return
		default:
		}

		rec := session.Record()
		msg := make([]byte, 1+len(rec.Body))
		msg[0] = MsgTypeChat
		copy(msg[1:], rec.Body)
		r.broadcastToAll(msg)
	}
}

func (r *Room) sharedStatsBroadcaster() {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			for _, p := range r.snapshotParticipants() {
				p.statsMu.Lock()
				stats := p.stats
				p.statsMu.Unlock()

				body, _ := json.Marshal(stats)
				msg := make([]byte, 1+len(body))
				msg[0] = MsgTypeStats
				copy(msg[1:], body)
				p.SendMessage(msg)
			}
		}
	}
}

func (p *Participant) Close() {
	p.cancel()
	_ = p.conn.Close()

	p.mediaProducer.Close()
	p.mediaSession.Close()
}

func (p *Participant) SendMessage(msg []byte) {
	if len(msg) == 0 {
		return
	}

	select {
	case <-p.ctx.Done():
		return
	default:
	}

	if isDroppableMessage(msg[0]) {
		select {
		case p.sendCh <- msg:
		default:
		}
		return
	}

	timer := time.NewTimer(reliableSendTimeout)
	defer timer.Stop()

	select {
	case <-p.ctx.Done():
	case p.sendCh <- msg:
	case <-timer.C:
		p.room.logger.Warn("closing slow participant", "user", p.userID, "room", p.room.id)
		p.cancel()
		_ = p.conn.Close()
	}
}

func (p *Participant) writePump() {
	for {
		select {
		case <-p.ctx.Done():
			return
		case msg := <-p.sendCh:
			if len(msg) == 0 {
				continue
			}
			if err := p.conn.SetWriteDeadline(time.Now().Add(participantWriteLimit)); err != nil {
				return
			}
			if err := p.conn.WriteMessage(websocket.BinaryMessage, msg); err != nil {
				p.cancel()
				_ = p.conn.Close()
				return
			}
		}
	}
}

func isDroppableMessage(msgType byte) bool {
	switch msgType {
	case MsgTypeVideo, MsgTypeAudio, MsgTypeScreen, MsgTypeStats:
		return true
	default:
		return false
	}
}

func (p *Participant) AppendMedia(msgType byte, payload []byte) {
	ts := uint64(time.Now().UnixMilli())

	_, err := p.mediaProducer.Submit(s2.AppendRecord{
		Body:      payload,
		Headers:   []s2.Header{s2.NewHeader("type", string([]byte{msgType}))},
		Timestamp: &ts,
	})
	if err != nil {
		p.room.logger.Error("append failed", "user", p.userID, "type", msgType, "error", err)
		return
	}

	// Track stats.
	p.statsMu.Lock()
	switch msgType {
	case MsgTypeVideo:
		p.stats.VideoRecords++
		p.stats.VideoBytes += uint64(len(payload))
	case MsgTypeAudio:
		p.stats.AudioRecords++
		p.stats.AudioBytes += uint64(len(payload))
	case MsgTypeScreen:
		p.stats.ScreenRecords++
		p.stats.ScreenBytes += uint64(len(payload))
	}
	p.statsMu.Unlock()
}

func mediaTypeFromHeaders(headers []s2.Header) byte {
	for _, h := range headers {
		if string(h.Name) == "type" && len(h.Value) == 1 {
			return h.Value[0]
		}
	}
	return 0
}
