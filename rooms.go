package main

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"sync"
	"time"
)

// RoomRegistry manages room codes.
type RoomRegistry struct {
	rooms   map[string]*RoomMeta
	roomsMu sync.RWMutex
}

type RoomMeta struct {
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
	CreatedBy string    `json:"created_by"`
}

func NewRoomRegistry() *RoomRegistry {
	return &RoomRegistry{
		rooms: make(map[string]*RoomMeta),
	}
}

func generateRoomID() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// CreateRoom generates a room code.
func (rr *RoomRegistry) CreateRoom(createdBy string) *RoomMeta {
	roomID := generateRoomID()

	meta := &RoomMeta{
		ID:        roomID,
		CreatedAt: time.Now(),
		CreatedBy: createdBy,
	}

	rr.roomsMu.Lock()
	rr.rooms[roomID] = meta
	rr.roomsMu.Unlock()

	return meta
}

// HandleCreateRoom handles POST /api/rooms.
func (rr *RoomRegistry) HandleCreateRoom(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST required", http.StatusMethodNotAllowed)
		return
	}

	var body struct {
		Name string `json:"name"`
	}
	json.NewDecoder(r.Body).Decode(&body)

	createdBy := body.Name
	if createdBy == "" {
		createdBy = "anonymous"
	}

	meta := rr.CreateRoom(createdBy)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(meta)
}
