package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
	"github.com/s2-streamstore/s2-sdk-go/s2"
)

type streamCounter struct {
	records  atomic.Uint64
	bytes    atomic.Uint64
	lastSize atomic.Uint64
	seqNum   atomic.Uint64
}

type StreamStats struct {
	Name       string  `json:"name"`
	SeqNum     uint64  `json:"seq_num"`
	Records    uint64  `json:"records"`
	Bytes      uint64  `json:"bytes"`
	RecordsPS  float64 `json:"records_per_sec"`
	BytesPS    float64 `json:"bytes_per_sec"`
	LastRecordSize uint64 `json:"last_record_size"`
}

type InspectSnapshot struct {
	Timestamp int64         `json:"timestamp"`
	Streams   []StreamStats `json:"streams"`
}

func (h *Hub) HandleInspect(w http.ResponseWriter, r *http.Request) {
	roomID := r.URL.Query().Get("room")
	if roomID == "" {
		http.Error(w, "room query param required", http.StatusBadRequest)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.logger.Error("inspect ws upgrade failed", "error", err)
		return
	}
	defer conn.Close()

	room := h.getOrCreateRoom(roomID)
	participants, err := room.GetParticipantHistory()
	if err != nil || len(participants) == 0 {
		conn.WriteJSON(map[string]string{"error": "no streams found"})
		return
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

	type streamInfo struct {
		name string
	}
	var streams []streamInfo
	for _, pid := range participants {
		streams = append(streams,
			streamInfo{fmt.Sprintf("rooms/%s/media/%s", roomID, pid)},
		)
	}
	streams = append(streams,
		streamInfo{fmt.Sprintf("rooms/%s/meta", roomID)},
		streamInfo{fmt.Sprintf("rooms/%s/chat", roomID)},
	)

	counters := make([]*streamCounter, len(streams))
	for i := range counters {
		counters[i] = &streamCounter{}
	}

	var wg sync.WaitGroup
	for i, si := range streams {
		wg.Add(1)
		go func(idx int, name string) {
			defer wg.Done()
			tailStream(ctx, h.basin, name, counters[idx])
		}(i, si.name)
	}

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	prevRecords := make([]uint64, len(streams))
	prevBytes := make([]uint64, len(streams))
	prevTime := time.Now()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			dt := now.Sub(prevTime).Seconds()
			if dt == 0 {
				dt = 0.5
			}

			snapshot := InspectSnapshot{
				Timestamp: now.UnixMilli(),
			}

			for i, si := range streams {
				r := counters[i].records.Load()
				b := counters[i].bytes.Load()

				ss := StreamStats{
					Name:           si.name,
					SeqNum:         counters[i].seqNum.Load(),
					Records:        r,
					Bytes:          b,
					RecordsPS:      float64(r-prevRecords[i]) / dt,
					BytesPS:        float64(b-prevBytes[i]) / dt,
					LastRecordSize: counters[i].lastSize.Load(),
				}
				snapshot.Streams = append(snapshot.Streams, ss)

				prevRecords[i] = r
				prevBytes[i] = b
			}

			prevTime = now

			msg, _ := json.Marshal(snapshot)
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		}
	}
}

func tailStream(ctx context.Context, basin *s2.BasinClient, streamName string, c *streamCounter) {
	stream := basin.Stream(s2.StreamName(streamName))

	// Retry until stream exists.
	var session *s2.ReadSession
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		var err error
		session, err = stream.ReadSession(ctx, &s2.ReadOptions{
			SeqNum: s2.Uint64(0),
		})
		if err == nil {
			break
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(1 * time.Second):
		}
	}
	defer session.Close()

	for session.Next() {
		select {
		case <-ctx.Done():
			return
		default:
		}

		rec := session.Record()
		c.records.Add(1)
		c.bytes.Add(uint64(len(rec.Body)))
		c.lastSize.Store(uint64(len(rec.Body)))
		c.seqNum.Store(rec.SeqNum)
	}
}
