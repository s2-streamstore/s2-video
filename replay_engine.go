package main

import (
	"container/heap"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/gorilla/websocket"
	"github.com/s2-streamstore/s2-sdk-go/s2"
)

const (
	replayProgressIntervalMs = 250

	replayPriorityJoin = iota
	replayPriorityMedia
	replayPriorityControl
	replayPriorityChat
	replayPriorityLeave
)

type replaySnapshot struct {
	ActiveParticipants []string
	HandMessages       [][]byte
	ChatMessages       [][]byte
}

type replayEvent struct {
	timestamp uint64
	priority  int
	streamKey string
	seqNum    uint64
	wsMessage []byte
}

type replayEventSource interface {
	Key() string
	Next() (*replayEvent, error)
	Close() error
}

type mediaReplaySource struct {
	key           string
	participantID string
	session       *s2.ReadSession
}

type metaReplaySource struct {
	key     string
	focus   string
	session *s2.ReadSession
}

type chatReplaySource struct {
	key     string
	session *s2.ReadSession
}

type replayHeapItem struct {
	event  *replayEvent
	source replayEventSource
}

type replayEventHeap []*replayHeapItem

func (h replayEventHeap) Len() int { return len(h) }

func (h replayEventHeap) Less(i, j int) bool {
	left := h[i].event
	right := h[j].event
	if left.timestamp != right.timestamp {
		return left.timestamp < right.timestamp
	}
	if left.priority != right.priority {
		return left.priority < right.priority
	}
	if left.streamKey != right.streamKey {
		return left.streamKey < right.streamKey
	}
	return left.seqNum < right.seqNum
}

func (h replayEventHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *replayEventHeap) Push(x any) {
	*h = append(*h, x.(*replayHeapItem))
}

func (h *replayEventHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

func (h *Hub) buildReplayTimeline(ctx context.Context, room *Room) (TimelineResponse, error) {
	participants, err := room.GetParticipantHistory()
	if err != nil {
		return TimelineResponse{}, err
	}
	sort.Strings(participants)

	resp := TimelineResponse{
		Participants: participants,
	}

	metaStream := h.basin.Stream(s2.StreamName(fmt.Sprintf("rooms/%s/meta", room.id)))
	if session, err := metaStream.ReadSession(ctx, &s2.ReadOptions{
		SeqNum: s2.Uint64(0),
		Wait:   s2.Int32(0),
		Clamp:  s2.Bool(true),
	}); err == nil {
		defer session.Close()
		for session.Next() {
			rec := session.Record()
			var event MetaEvent
			if err := json.Unmarshal(rec.Body, &event); err != nil {
				continue
			}
			if event.Type != "join" && event.Type != "leave" {
				continue
			}
			resp.Events = append(resp.Events, TimelineEvent{
				Type:      event.Type,
				UserID:    event.UserID,
				Timestamp: int64(rec.Timestamp),
			})
		}
	}

	sort.Slice(resp.Events, func(i, j int) bool {
		if resp.Events[i].Timestamp != resp.Events[j].Timestamp {
			return resp.Events[i].Timestamp < resp.Events[j].Timestamp
		}
		if resp.Events[i].Type != resp.Events[j].Type {
			return resp.Events[i].Type < resp.Events[j].Type
		}
		return resp.Events[i].UserID < resp.Events[j].UserID
	})

	start, end, err := h.computeReplayBounds(ctx, room, participants)
	if err == nil {
		resp.Start = int64(start)
		resp.End = int64(end)
	}

	return resp, nil
}

func (h *Hub) computeReplayBounds(ctx context.Context, room *Room, participants []string) (uint64, uint64, error) {
	var (
		fallbackStart uint64
		mediaStart    uint64
		end           uint64
		hasData       bool
		hasMedia      bool
	)

	inspectStream := func(streamName string, isMedia bool) {
		stream := h.basin.Stream(s2.StreamName(streamName))
		if batch, err := stream.Read(ctx, &s2.ReadOptions{
			SeqNum: s2.Uint64(0),
			Count:  s2.Uint64(1),
			Wait:   s2.Int32(0),
			Clamp:  s2.Bool(true),
		}); err == nil && len(batch.Records) > 0 {
			ts := batch.Records[0].Timestamp
			if !hasData || ts < fallbackStart {
				fallbackStart = ts
			}
			if isMedia && (!hasMedia || ts < mediaStart) {
				mediaStart = ts
				hasMedia = true
			}
			hasData = true
		}

		if tail, err := stream.CheckTail(ctx); err == nil {
			ts := tail.Tail.Timestamp
			if !hasData || ts > end {
				end = ts
			}
			hasData = true
		}
	}

	inspectStream(fmt.Sprintf("rooms/%s/meta", room.id), false)
	inspectStream(fmt.Sprintf("rooms/%s/chat", room.id), false)
	for _, participant := range participants {
		inspectStream(fmt.Sprintf("rooms/%s/media/%s", room.id, participant), true)
	}

	if !hasData {
		return 0, 0, fmt.Errorf("no replay data found")
	}

	start := fallbackStart
	if hasMedia {
		start = mediaStart
	}

	return start, end, nil
}

func (h *Hub) buildReplaySnapshot(ctx context.Context, room *Room, startTs uint64, participants []string, focus string) (*replaySnapshot, error) {
	snapshot := &replaySnapshot{}
	if startTs == 0 {
		return snapshot, nil
	}

	active := make(map[string]bool)
	handsRaised := make(map[string]bool)
	metaStream := h.basin.Stream(s2.StreamName(fmt.Sprintf("rooms/%s/meta", room.id)))
	if session, err := metaStream.ReadSession(ctx, &s2.ReadOptions{
		SeqNum: s2.Uint64(0),
		Wait:   s2.Int32(0),
		Clamp:  s2.Bool(true),
	}); err == nil {
		defer session.Close()
		for session.Next() {
			rec := session.Record()
			if rec.Timestamp >= startTs {
				break
			}

			var event MetaEvent
			if err := json.Unmarshal(rec.Body, &event); err != nil {
				continue
			}

			switch event.Type {
			case "join":
				active[event.UserID] = true
			case "leave":
				delete(active, event.UserID)
				delete(handsRaised, event.UserID)
			case "control":
				var control struct {
					Type   string `json:"type"`
					UserID string `json:"user_id"`
					Raised bool   `json:"raised"`
				}
				if err := json.Unmarshal(event.Data, &control); err != nil {
					continue
				}
				if control.Type == "hand" {
					if control.Raised {
						handsRaised[control.UserID] = true
					} else {
						delete(handsRaised, control.UserID)
					}
				}
			}
		}
	}

	for _, participant := range participants {
		if !active[participant] {
			continue
		}
		if focus != "" && participant != focus {
			continue
		}
		snapshot.ActiveParticipants = append(snapshot.ActiveParticipants, participant)
	}

	for _, participant := range snapshot.ActiveParticipants {
		if !handsRaised[participant] {
			continue
		}
		snapshot.HandMessages = append(snapshot.HandMessages, encodeReplayHandMessage(participant, true, startTs))
	}

	chatStream := h.basin.Stream(s2.StreamName(fmt.Sprintf("rooms/%s/chat", room.id)))
	if session, err := chatStream.ReadSession(ctx, &s2.ReadOptions{
		SeqNum: s2.Uint64(0),
		Wait:   s2.Int32(0),
		Clamp:  s2.Bool(true),
	}); err == nil {
		defer session.Close()
		for session.Next() {
			rec := session.Record()
			if rec.Timestamp >= startTs {
				break
			}
			msg := make([]byte, 1+len(rec.Body))
			msg[0] = MsgTypeChat
			copy(msg[1:], rec.Body)
			snapshot.ChatMessages = append(snapshot.ChatMessages, msg)
		}
	}

	return snapshot, nil
}

func encodeReplayHandMessage(userID string, raised bool, timestamp uint64) []byte {
	body, _ := json.Marshal(map[string]any{
		"type":      "hand",
		"user_id":   userID,
		"raised":    raised,
		"timestamp": int64(timestamp),
	})
	msg := make([]byte, 1+len(body))
	msg[0] = MsgTypeControl
	copy(msg[1:], body)
	return msg
}

func augmentReplayControlPayload(data json.RawMessage, timestamp uint64) []byte {
	if len(data) == 0 {
		return nil
	}

	var payload map[string]any
	if err := json.Unmarshal(data, &payload); err == nil {
		if _, exists := payload["timestamp"]; !exists {
			payload["timestamp"] = int64(timestamp)
		}
		if body, err := json.Marshal(payload); err == nil {
			return body
		}
	}

	return data
}

func openReplayReadSession(ctx context.Context, stream *s2.StreamClient, startTs uint64) (*s2.ReadSession, error) {
	readOpts := &s2.ReadOptions{
		Wait:  s2.Int32(0),
		Clamp: s2.Bool(true),
	}
	if startTs > 0 {
		readOpts.Timestamp = &startTs
	} else {
		readOpts.SeqNum = s2.Uint64(0)
	}
	return stream.ReadSession(ctx, readOpts)
}

func (h *Hub) openReplaySources(ctx context.Context, room *Room, participants []string, startTs uint64, focus string) []replayEventSource {
	sources := make([]replayEventSource, 0, len(participants)+2)

	for _, participant := range participants {
		stream := h.basin.Stream(s2.StreamName(fmt.Sprintf("rooms/%s/media/%s", room.id, participant)))
		session, err := openReplayReadSession(ctx, stream, startTs)
		if err != nil {
			h.logger.Error("replay media read session failed", "stream", fmt.Sprintf("rooms/%s/media/%s", room.id, participant), "error", err)
			continue
		}
		sources = append(sources, &mediaReplaySource{
			key:           "media:" + participant,
			participantID: participant,
			session:       session,
		})
	}

	metaStream := h.basin.Stream(s2.StreamName(fmt.Sprintf("rooms/%s/meta", room.id)))
	if session, err := openReplayReadSession(ctx, metaStream, startTs); err == nil {
		sources = append(sources, &metaReplaySource{
			key:     "meta",
			focus:   focus,
			session: session,
		})
	}

	chatStream := h.basin.Stream(s2.StreamName(fmt.Sprintf("rooms/%s/chat", room.id)))
	if session, err := openReplayReadSession(ctx, chatStream, startTs); err == nil {
		sources = append(sources, &chatReplaySource{
			key:     "chat",
			session: session,
		})
	}

	return sources
}

func (s *mediaReplaySource) Key() string { return s.key }

func (s *mediaReplaySource) Next() (*replayEvent, error) {
	for s.session.Next() {
		rec := s.session.Record()
		mediaType := mediaTypeFromHeaders(rec.Headers)
		if mediaType == 0 {
			continue
		}
		return &replayEvent{
			timestamp: rec.Timestamp,
			priority:  replayPriorityMedia,
			streamKey: s.key,
			seqNum:    rec.SeqNum,
			wsMessage: EncodeMediaMessage(mediaType, s.participantID, rec.Body),
		}, nil
	}
	if err := s.session.Err(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

func (s *mediaReplaySource) Close() error {
	return s.session.Close()
}

func (s *metaReplaySource) Key() string { return s.key }

func (s *metaReplaySource) Next() (*replayEvent, error) {
	for s.session.Next() {
		rec := s.session.Record()

		var event MetaEvent
		if err := json.Unmarshal(rec.Body, &event); err != nil {
			continue
		}

		if s.focus != "" && event.UserID != s.focus {
			continue
		}

		switch event.Type {
		case "join":
			msg, _ := EncodeControlMessage(ControlMessage{Type: ControlJoin, UserID: event.UserID, Timestamp: int64(rec.Timestamp)})
			return &replayEvent{timestamp: rec.Timestamp, priority: replayPriorityJoin, streamKey: s.key + ":join:" + event.UserID, seqNum: rec.SeqNum, wsMessage: msg}, nil
		case "leave":
			msg, _ := EncodeControlMessage(ControlMessage{Type: ControlLeave, UserID: event.UserID, Timestamp: int64(rec.Timestamp)})
			return &replayEvent{timestamp: rec.Timestamp, priority: replayPriorityLeave, streamKey: s.key + ":leave:" + event.UserID, seqNum: rec.SeqNum, wsMessage: msg}, nil
		case "control":
			payload := augmentReplayControlPayload(event.Data, rec.Timestamp)
			if len(payload) == 0 {
				continue
			}
			msg := make([]byte, 1+len(payload))
			msg[0] = MsgTypeControl
			copy(msg[1:], payload)
			return &replayEvent{timestamp: rec.Timestamp, priority: replayPriorityControl, streamKey: s.key + ":control:" + event.UserID, seqNum: rec.SeqNum, wsMessage: msg}, nil
		}
	}
	if err := s.session.Err(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

func (s *metaReplaySource) Close() error {
	return s.session.Close()
}

func (s *chatReplaySource) Key() string { return s.key }

func (s *chatReplaySource) Next() (*replayEvent, error) {
	for s.session.Next() {
		rec := s.session.Record()
		msg := make([]byte, 1+len(rec.Body))
		msg[0] = MsgTypeChat
		copy(msg[1:], rec.Body)
		return &replayEvent{timestamp: rec.Timestamp, priority: replayPriorityChat, streamKey: s.key, seqNum: rec.SeqNum, wsMessage: msg}, nil
	}
	if err := s.session.Err(); err != nil {
		return nil, err
	}
	return nil, io.EOF
}

func (s *chatReplaySource) Close() error {
	return s.session.Close()
}

func closeReplaySources(sources []replayEventSource) {
	for _, source := range sources {
		_ = source.Close()
	}
}

func sendReplayMessage(conn *websocket.Conn, msg []byte) error {
	if len(msg) == 0 {
		return nil
	}
	return conn.WriteMessage(websocket.BinaryMessage, msg)
}

func sendReplayProgress(conn *websocket.Conn, timestamp uint64) error {
	msg, _ := EncodeControlMessage(ControlMessage{Type: ControlReplayTime, Timestamp: int64(timestamp)})
	return sendReplayMessage(conn, msg)
}

func (h *Hub) streamReplay(conn *websocket.Conn, ctx context.Context, room *Room, participants []string, startTs, roomEnd uint64, speed float64, focus string) error {
	sources := h.openReplaySources(ctx, room, participants, startTs, focus)
	defer closeReplaySources(sources)

	eventHeap := &replayEventHeap{}
	heap.Init(eventHeap)

	for _, source := range sources {
		event, err := source.Next()
		if err == nil {
			heap.Push(eventHeap, &replayHeapItem{event: event, source: source})
			continue
		}
		if err != io.EOF {
			h.logger.Error("failed to prime replay source", "source", source.Key(), "error", err)
		}
	}

	clock := &replayClock{
		wallStart: time.Now(),
		originTs:  startTs,
		speed:     speed,
	}
	lastProgressTs := startTs

	for eventHeap.Len() > 0 {
		item := heap.Pop(eventHeap).(*replayHeapItem)
		event := item.event

		if !clock.waitUntil(ctx, event.timestamp) {
			return ctx.Err()
		}
		if err := sendReplayMessage(conn, event.wsMessage); err != nil {
			return err
		}
		if event.timestamp >= lastProgressTs+replayProgressIntervalMs {
			if err := sendReplayProgress(conn, event.timestamp); err != nil {
				return err
			}
			lastProgressTs = event.timestamp
		}

		nextEvent, err := item.source.Next()
		if err == nil {
			item.event = nextEvent
			heap.Push(eventHeap, item)
			continue
		}
		if err != io.EOF {
			h.logger.Error("replay source failed", "source", item.source.Key(), "error", err)
			return err
		}
	}

	finalTs := roomEnd
	if finalTs < startTs {
		finalTs = startTs
	}
	if lastProgressTs != finalTs {
		if err := sendReplayProgress(conn, finalTs); err != nil {
			return err
		}
	}

	complete, _ := EncodeControlMessage(ControlMessage{Type: ControlReplayComplete, Timestamp: int64(finalTs)})
	return sendReplayMessage(conn, complete)
}
