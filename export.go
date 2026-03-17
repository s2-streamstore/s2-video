package main

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"sync"

	"github.com/s2-streamstore/s2-sdk-go/s2"
)

var exportSemaphore = make(chan struct{}, 4)

func (h *Hub) HandleExport(w http.ResponseWriter, r *http.Request) {
	select {
	case exportSemaphore <- struct{}{}:
		defer func() { <-exportSemaphore }()
	default:
		http.Error(w, "too many concurrent exports, try again later", http.StatusServiceUnavailable)
		return
	}

	path := strings.TrimPrefix(r.URL.Path, "/api/rooms/")
	roomID := strings.TrimSuffix(path, "/export")
	if roomID == "" {
		http.Error(w, "room ID required", http.StatusBadRequest)
		return
	}

	room := h.getOrCreateRoom(roomID)
	participants, err := room.GetParticipantHistory()
	if err != nil || len(participants) == 0 {
		http.Error(w, "no recordings found", http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "video/mp4")
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s.mp4"`, roomID))

	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()

	if err := exportComposite(ctx, h.basin, h.logger, roomID, participants, w); err != nil {
		h.logger.Error("export failed", "room", roomID, "error", err)
	}
}

func exportComposite(ctx context.Context, basin *s2.BasinClient, logger *slog.Logger, roomID string, participants []string, out io.Writer) error {
	n := len(participants)

	cols := int(math.Ceil(math.Sqrt(float64(n))))
	rows := int(math.Ceil(float64(n) / float64(cols)))
	tileW := 640
	tileH := 480
	outW := cols * tileW
	outH := rows * tileH

	var args []string
	var videoPipes []*os.File
	var videoWriters []*os.File
	var audioPipes []*os.File
	var audioWriters []*os.File

	for i := 0; i < n; i++ {
		vr, vw, err := os.Pipe()
		if err != nil {
			return fmt.Errorf("video pipe %d: %w", i, err)
		}
		videoPipes = append(videoPipes, vr)
		videoWriters = append(videoWriters, vw)

		ar, aw, err := os.Pipe()
		if err != nil {
			return fmt.Errorf("audio pipe %d: %w", i, err)
		}
		audioPipes = append(audioPipes, ar)
		audioWriters = append(audioWriters, aw)
	}

	var extraFiles []*os.File
	for i := 0; i < n; i++ {
		fd := 3 + len(extraFiles)
		args = append(args, "-f", "image2pipe", "-framerate", "30", "-i", fmt.Sprintf("/dev/fd/%d", fd))
		extraFiles = append(extraFiles, videoPipes[i])

		fd = 3 + len(extraFiles)
		args = append(args, "-f", "s16le", "-ar", "16000", "-ac", "1", "-i", fmt.Sprintf("/dev/fd/%d", fd))
		extraFiles = append(extraFiles, audioPipes[i])
	}

	var filterParts []string
	var layoutParts []string
	for i := 0; i < n; i++ {
		filterParts = append(filterParts, fmt.Sprintf("[%d:v]scale=%d:%d,setpts=PTS-STARTPTS[v%d]", i*2, tileW, tileH, i))
		col := i % cols
		row := i / cols
		if i == 0 {
			layoutParts = append(layoutParts, "0_0")
		} else {
			layoutParts = append(layoutParts, fmt.Sprintf("%d_%d", col*tileW, row*tileH))
		}
	}

	for i := n; i < cols*rows; i++ {
		filterParts = append(filterParts, fmt.Sprintf("nullsrc=size=%dx%d:duration=99999:rate=30[v%d]", tileW, tileH, i))
		col := i % cols
		row := i / cols
		layoutParts = append(layoutParts, fmt.Sprintf("%d_%d", col*tileW, row*tileH))
	}

	totalTiles := cols * rows
	var xstackInputs []string
	for i := 0; i < totalTiles; i++ {
		xstackInputs = append(xstackInputs, fmt.Sprintf("[v%d]", i))
	}

	filterComplex := strings.Join(filterParts, ";") + ";" +
		strings.Join(xstackInputs, "") +
		fmt.Sprintf("xstack=inputs=%d:layout=%s[vout]", totalTiles, strings.Join(layoutParts, "|"))

	if n > 1 {
		var amixInputs string
		for i := 0; i < n; i++ {
			amixInputs += fmt.Sprintf("[%d:a]", i*2+1)
		}
		filterComplex += ";" + amixInputs + fmt.Sprintf("amix=inputs=%d:duration=longest[aout]", n)
	} else {
		filterComplex += ";[1:a]anull[aout]"
	}

	args = append(args,
		"-filter_complex", filterComplex,
		"-map", "[vout]", "-map", "[aout]",
		"-c:v", "libx264", "-preset", "ultrafast", "-pix_fmt", "yuv420p",
		"-s", fmt.Sprintf("%dx%d", outW, outH),
		"-c:a", "aac", "-b:a", "128k",
		"-movflags", "frag_keyframe+empty_moov",
		"-f", "mp4", "pipe:1",
	)

	cmd := exec.CommandContext(ctx, "ffmpeg", args...)
	cmd.ExtraFiles = extraFiles
	cmd.Stdout = out

	if err := cmd.Start(); err != nil {
		closeAll(videoPipes, videoWriters, audioPipes, audioWriters)
		return fmt.Errorf("ffmpeg start: %w", err)
	}

	for _, f := range videoPipes {
		f.Close()
	}
	for _, f := range audioPipes {
		f.Close()
	}

	var wg sync.WaitGroup

	for i, pid := range participants {
		vw := videoWriters[i]
		aw := audioWriters[i]

		wg.Add(1)
		go func(pid string, videoPipe, audioPipe *os.File) {
			defer wg.Done()
			defer videoPipe.Close()
			defer audioPipe.Close()
			demuxMediaStream(ctx, basin, logger, fmt.Sprintf("rooms/%s/media/%s", roomID, pid), videoPipe, audioPipe)
		}(pid, vw, aw)
	}

	wg.Wait()
	return cmd.Wait()
}

func demuxMediaStream(ctx context.Context, basin *s2.BasinClient, logger *slog.Logger, streamName string, videoOut, audioOut io.Writer) {
	stream := basin.Stream(s2.StreamName(streamName))

	session, err := stream.ReadSession(ctx, &s2.ReadOptions{
		SeqNum: s2.Uint64(0),
		Wait:   s2.Int32(0),
		Clamp:  s2.Bool(true),
	})
	if err != nil {
		logger.Error("export read failed", "stream", streamName, "error", err)
		return
	}
	defer session.Close()

	for session.Next() {
		rec := session.Record()
		mediaType := mediaTypeFromHeaders(rec.Headers)

		switch mediaType {
		case MsgTypeVideo, MsgTypeScreen:
			if _, err := videoOut.Write(rec.Body); err != nil {
				return
			}
		case MsgTypeAudio:
			if _, err := audioOut.Write(rec.Body); err != nil {
				return
			}
		}
	}
}

func closeAll(slices ...[]*os.File) {
	for _, s := range slices {
		for _, f := range s {
			f.Close()
		}
	}
}
