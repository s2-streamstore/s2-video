package main

import (
	"encoding/json"
	"net/http"
	"strings"
)

type TimelineResponse struct {
	Start        int64           `json:"start"`
	End          int64           `json:"end"`
	Participants []string        `json:"participants"`
	Events       []TimelineEvent `json:"events"`
}

type TimelineEvent struct {
	Type      string `json:"type"`
	UserID    string `json:"user_id"`
	Timestamp int64  `json:"timestamp"`
}

func (h *Hub) HandleTimeline(w http.ResponseWriter, r *http.Request) {
	path := strings.TrimPrefix(r.URL.Path, "/api/rooms/")
	roomID := strings.TrimSuffix(path, "/timeline")
	if roomID == "" {
		http.Error(w, "room ID required", http.StatusBadRequest)
		return
	}

	room := h.getOrCreateRoom(roomID)
	resp, err := h.buildReplayTimeline(r.Context(), room)
	if err != nil {
		http.Error(w, "failed to build replay timeline", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}
