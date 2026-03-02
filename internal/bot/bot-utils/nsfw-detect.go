package botutils

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"

	"github.com/biisal/fast-stream-bot/internal/types"
	"github.com/gotd/td/telegram"
	"github.com/gotd/td/tg"
)

// DetectNSFW downloads a middle chunk of the video in multiple small requests,
// extracts a frame, and checks if it's NSFW.
func DetectNSFW(ctx context.Context, fileMeta *types.File, client *telegram.Client) bool {
	const (
		maxLimit  = 512 * 1024 // 512 KB max per MTProto request
		blockSize = 4096       // offsets must be aligned
		targetMB  = 8          // aim to download 8 MB
	)

	targetSize := int64(targetMB * 1024 * 1024)
	if targetSize > fileMeta.Size {
		targetSize = fileMeta.Size
	}

	mid := fileMeta.Size / 2
	start := mid - targetSize/2
	if start < 0 {
		start = 0
	}
	start = (start / blockSize) * blockSize // align offset

	if start+targetSize > fileMeta.Size {
		targetSize = fileMeta.Size - start
	}

	slog.Info("Downloading middle content", "start", start, "targetSize", targetSize)

	var allBytes []byte
	offset := start
	for int64(len(allBytes)) < targetSize && offset < fileMeta.Size {
		limit := targetSize - int64(len(allBytes))
		if limit > maxLimit {
			limit = maxLimit
		}
		// Clamp to remaining bytes
		if offset+limit > fileMeta.Size {
			limit = fileMeta.Size - offset
		}
		if limit <= 0 {
			break
		}

		req := &tg.UploadGetFileRequest{
			Location: fileMeta.Location,
			Offset:   offset,
			Limit:    int(limit),
		}

		res, err := client.API().UploadGetFile(ctx, req)
		if err != nil {
			slog.Error("Failed to download chunk", "offset", offset, "limit", limit, "error", err)
			return false
		}

		file, ok := res.(*tg.UploadFile)
		if !ok {
			slog.Error("Unexpected response type", "type", fmt.Sprintf("%T", res))
			return false
		}

		allBytes = append(allBytes, file.Bytes...)
		offset += int64(len(file.Bytes))
		if len(file.Bytes) == 0 {
			break
		}
	}

	if len(allBytes) == 0 {
		slog.Error("No bytes downloaded from middle chunk")
		return false
	}

	// Write temp video
	tmpVid, err := os.CreateTemp("", "video_*.mp4")
	if err != nil {
		slog.Error("Failed to create temp video", "error", err)
		return false
	}
	defer func() {
		if err = os.Remove(tmpVid.Name()); err != nil {
			slog.Warn("Failed to remove temp video", "path", tmpVid.Name(), "error", err)
		}
	}()
	defer func() {
		if err = tmpVid.Close(); err != nil {
			slog.Warn("Failed to close temp video", "error", err)
		}
	}()

	if _, err := tmpVid.Write(allBytes); err != nil {
		slog.Error("Failed to write temp video", "error", err)
		return false
	}
	if err = tmpVid.Sync(); err != nil {
		slog.Warn("Failed to sync temp video", "error", err)
	}

	// Temp image
	tmpImg, err := os.CreateTemp("", "frame_*.jpg")
	if err != nil {
		slog.Error("Failed to create temp image", "error", err)
		return false
	}
	imgPath := tmpImg.Name()
	if err := tmpImg.Close(); err != nil {
		slog.Warn("Failed to close temp image", "error", err)
	}
	defer func() {
		if err := os.Remove(imgPath); err != nil {
			slog.Warn("Failed to remove temp image", "path", imgPath, "error", err)
		}
	}()

	// Try middle timestamp first
	if !extractFrameAtTime(tmpVid.Name(), imgPath, "00:00:02") {
		if !extractFirstAvailableFrame(tmpVid.Name(), imgPath) {
			slog.Error("Failed to extract any frame")
			return false
		}
	}

	return checkImgIsNsfw(imgPath)
}

// --- helper functions remain the same ---
func extractFrameAtTime(videoPath, imgPath, timestamp string) bool {
	cmd := exec.Command("ffmpeg", "-y", "-i", videoPath, "-ss", timestamp, "-vframes", "1", "-q:v", "2", imgPath)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err == nil {
		return true
	}

	cmd2 := exec.Command("ffmpeg", "-y", "-ss", timestamp, "-i", videoPath, "-frames:v", "1", "-q:v", "2", imgPath)
	cmd2.Stderr = &stderr
	if err := cmd2.Run(); err != nil {
		slog.Warn("Failed to extract frame at time", "timestamp", timestamp, "stderr", stderr.String())
		return false
	}
	return true
}

func extractFirstAvailableFrame(videoPath, imgPath string) bool {
	cmd := exec.Command("ffmpeg", "-y", "-i", videoPath, "-vframes", "1", "-q:v", "2", imgPath)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		slog.Error("Failed to extract first frame", "stderr", stderr.String())
		return false
	}
	slog.Info("Extracted first available frame")
	return true
}

func checkImgIsNsfw(imgPath string) bool {
	stat, err := os.Stat(imgPath)
	if err != nil || stat.Size() == 0 {
		slog.Error("Invalid extracted image", "path", imgPath, "error", err)
		return false
	}
	slog.Info("Image ready for NSFW check", "path", imgPath, "size", stat.Size())
	// TODO: plug in real NSFW detector
	return false
}
