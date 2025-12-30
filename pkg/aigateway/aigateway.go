// Package aigateway provides client functionality for communicating with
// AI transcription gateways. It handles session management, media publishing
// via RTMP or WHIP, and SSE-based transcript event streaming.
package aigateway

import (
	"bufio"
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"stream.place/streamplace/pkg/log"
)

const (
	// DefaultStreamTimeout is the default timeout for AI gateway stream sessions.
	DefaultStreamTimeout = 120

	// StopStreamTimeout is the timeout for stopping a stream session.
	StopStreamTimeout = 5

	// maxResponseBodySize limits how much of an error response body we read.
	maxResponseBodySize = 8 << 10 // 8KB

	// sseBufferSize is the initial buffer size for SSE scanning.
	sseBufferSize = 64 * 1024

	// sseMaxBufferSize is the maximum buffer size for SSE scanning.
	sseMaxBufferSize = 1024 * 1024
)

// Config holds the configuration for connecting to an AI gateway.
type Config struct {
	// BaseURL is the base URL of the AI gateway (e.g., "http://localhost:5937").
	BaseURL string

	// PathPrefix is an optional path prefix for gateway requests (e.g., "gateway").
	PathPrefix string

	// Pipeline is the AI pipeline capability name (e.g., "transcriber").
	Pipeline string

	// RTMPHost is the host:port for RTMP media ingress if not provided by gateway.
	RTMPHost string
}

// Session represents an active AI gateway transcription session.
type Session struct {
	// ID is the unique identifier for this session.
	ID string

	// StopURL is the URL for stopping the session (if provided by gateway).
	StopURL string

	// StatusURL is the URL to check session status.
	StatusURL string

	// DataURL is the SSE endpoint for receiving transcript events.
	DataURL string

	// UpdateURL is the URL for sending session updates.
	UpdateURL string

	// WhipURL is the WHIP endpoint for WebRTC media ingress (if available).
	WhipURL string

	// WhepURL is the WHEP endpoint for WebRTC media egress (if available).
	WhepURL string

	// RTMPURL is the RTMP endpoint for media ingress (if available).
	RTMPURL string
}

type streamStartRequest struct {
	StreamName string `json:"stream_name"`
	Params     string `json:"params"`
	StreamID   string `json:"stream_id"`
	RTMPOutput string `json:"rtmp_output"`
}

type streamStartResponse struct {
	StatusURL string `json:"status_url"`
	DataURL   string `json:"data_url"`
	UpdateURL string `json:"update_url"`
	WhipURL   string `json:"whip_url"`
	WhepURL   string `json:"whep_url"`
	RTMPURL   string `json:"rtmp_url"`
	StopURL   string `json:"stop_url"`
	StreamID  string `json:"stream_id"`
}

type startParams struct {
	EnableVideoIngress bool `json:"enable_video_ingress"`
	EnableVideoEgress  bool `json:"enable_video_egress"`
	EnableDataOutput   bool `json:"enable_data_output"`
}

// envelope wraps request parameters in the Livepeer gateway header format.
type envelope struct {
	Request        string `json:"request"`
	ParametersJSON string `json:"parameters"`
	Capability     string `json:"capability"`
	TimeoutSeconds int    `json:"timeout_seconds"`
}

// StartStream initiates a new transcription session with the AI gateway.
// It returns a Session containing the endpoints for media ingress and transcript output.
func StartStream(ctx context.Context, cfg Config, streamName string) (*Session, error) {
	prefix := ""
	if cfg.PathPrefix != "" {
		prefix = "/" + strings.Trim(cfg.PathPrefix, "/")
	}
	base := strings.TrimRight(cfg.BaseURL, "/")
	candidates := []string{
		base + prefix + "/process/stream/start",
		base + "/gateway/process/stream/start",
		base + prefix + "/ai/stream/start",
	}

	env := envelope{
		Request:        "{}",
		ParametersJSON: mustJSON(startParams{EnableVideoIngress: true, EnableVideoEgress: true, EnableDataOutput: true}),
		Capability:     cfg.Pipeline,
		TimeoutSeconds: DefaultStreamTimeout,
	}
	envBytes, err := json.Marshal(env)
	if err != nil {
		return nil, fmt.Errorf("marshal envelope: %w", err)
	}
	livepeerHeader := base64.StdEncoding.EncodeToString(envBytes)

	paramsObj := map[string]any{
		"height": 720,
		"width":  1280,
	}
	paramsJSON := mustJSON(paramsObj)

	body := streamStartRequest{
		StreamName: streamName,
		Params:     paramsJSON,
		StreamID:   "",
		RTMPOutput: "",
	}
	bodyBytes, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("marshal request body: %w", err)
	}

	var lastBody string
	var lastStatus string
	var lastErr error
	for _, startURL := range candidates {
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, startURL, bytes.NewReader(bodyBytes))
		if err != nil {
			return nil, fmt.Errorf("new request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Livepeer", livepeerHeader)

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodySize))
		_ = resp.Body.Close()
		if resp.StatusCode == http.StatusNotFound {
			lastStatus = resp.Status
			lastBody = string(b)
			continue
		}
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			return nil, fmt.Errorf("start stream failed: %s: %s", resp.Status, string(b))
		}

		var sr streamStartResponse
		if err := json.Unmarshal(b, &sr); err != nil {
			return nil, fmt.Errorf("decode response: %w", err)
		}

		if sr.StreamID == "" {
			return nil, fmt.Errorf("start response missing stream_id")
		}

		session := &Session{
			ID:        sr.StreamID,
			StopURL:   sr.StopURL,
			StatusURL: sr.StatusURL,
			DataURL:   sr.DataURL,
			UpdateURL: sr.UpdateURL,
			WhipURL:   sr.WhipURL,
			WhepURL:   sr.WhepURL,
			RTMPURL:   sr.RTMPURL,
		}
		baseURL := strings.TrimRight(cfg.BaseURL, "/")
		session.StopURL = normalizeGatewayURL(baseURL, session.StopURL)
		session.StatusURL = normalizeGatewayURL(baseURL, session.StatusURL)
		session.DataURL = normalizeGatewayURL(baseURL, session.DataURL)
		session.UpdateURL = normalizeGatewayURL(baseURL, session.UpdateURL)
		session.WhipURL = normalizeGatewayURL(baseURL, session.WhipURL)
		session.WhepURL = normalizeGatewayURL(baseURL, session.WhepURL)
		session.RTMPURL = normalizeGatewayURL(baseURL, session.RTMPURL)

		return session, nil
	}
	if lastErr != nil {
		return nil, fmt.Errorf("do request: %w", lastErr)
	}
	return nil, fmt.Errorf("start stream failed: %s: %s", lastStatus, lastBody)
}

// StopStream terminates an active transcription session.
func StopStream(ctx context.Context, cfg Config, streamID string) error {
	if streamID == "" {
		return fmt.Errorf("empty streamID")
	}

	prefix := ""
	if cfg.PathPrefix != "" {
		prefix = "/" + strings.Trim(cfg.PathPrefix, "/")
	}
	stopURL := strings.TrimRight(cfg.BaseURL, "/") + prefix + "/ai/stream/" + streamID + "/stop"

	env := envelope{
		Request:        mustJSON(map[string]string{"stream_id": streamID}),
		ParametersJSON: mustJSON(map[string]any{}),
		Capability:     cfg.Pipeline,
		TimeoutSeconds: StopStreamTimeout,
	}
	envBytes, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal envelope: %w", err)
	}
	livepeerHeader := base64.StdEncoding.EncodeToString(envBytes)

	bodyObj := map[string]string{"stream_id": streamID}
	bodyBytes, err := json.Marshal(bodyObj)
	if err != nil {
		return fmt.Errorf("marshal body: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, stopURL, bytes.NewReader(bodyBytes))
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Livepeer", livepeerHeader)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodySize))
		return fmt.Errorf("stop stream failed: %s: %s", resp.Status, string(b))
	}

	return nil
}

func normalizeGatewayURL(base, raw string) string {
	if raw == "" || base == "" {
		return raw
	}
	baseURL, err := url.Parse(base)
	if err != nil {
		return raw
	}
	if baseURL.Scheme == "" || baseURL.Host == "" {
		return raw
	}
	if strings.HasPrefix(raw, "/") {
		ref, err := url.Parse(raw)
		if err != nil {
			return raw
		}
		return baseURL.ResolveReference(ref).String()
	}
	u, err := url.Parse(raw)
	if err != nil {
		return raw
	}
	if u.Scheme == "" || u.Host == "" {
		ref, err := url.Parse(raw)
		if err != nil {
			return raw
		}
		return baseURL.ResolveReference(ref).String()
	}
	u.Scheme = baseURL.Scheme
	u.Host = baseURL.Host
	return u.String()
}

func StopStreamURL(ctx context.Context, stopURL string) error {
	if stopURL == "" {
		return fmt.Errorf("empty stopURL")
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, stopURL, bytes.NewReader([]byte("{}")))
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodySize))
		return fmt.Errorf("stop stream failed: %s: %s", resp.Status, string(b))
	}
	return nil
}

// ConstructRTMPURL builds an RTMP URL using the provided host and the session ID.
func (s *Session) ConstructRTMPURL(rtmpHost string) string {
	return fmt.Sprintf("rtmp://%s/%s", rtmpHost, s.ID)
}

func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// TranscriptEvent represents a single transcription event from the AI gateway.
type TranscriptEvent struct {
	// Type is the event type (e.g., "transcript").
	Type string `json:"type"`

	// Timing contains audio window and media clock information for rebasing.
	Timing *Timing `json:"timing,omitempty"`

	// Stats contains optional performance statistics for this transcription.
	Stats *Stats `json:"stats,omitempty"`

	// ReceivedAt is when Streamplace received this event (not from JSON).
	ReceivedAt time.Time `json:"-"`

	// Segments contains structured transcript segments with explicit media-clock timestamps.
	Segments []TranscriptSegment `json:"segments,omitempty"`
}

// TranscriptSegment represents a timed subtitle unit (phrase/line) in media-clock time.
// Times are stream-relative milliseconds.
type TranscriptSegment struct {
	ID      string          `json:"id"`
	StartMS int64           `json:"start_ms"`
	EndMS   int64           `json:"end_ms"`
	Text    string          `json:"text"`
	Words   []WordTimestamp `json:"words,omitempty"`
}

// WordTimestamp is an optional word-level timing payload.
// Times are stream-relative milliseconds.
type WordTimestamp struct {
	StartMS int64  `json:"start_ms"`
	EndMS   int64  `json:"end_ms"`
	Text    string `json:"text"`
}

// Timing contains timing metadata for transcript events.
type Timing struct {
	// MediaWindowStartMS is the absolute media time (after rebasing) in milliseconds.
	MediaWindowStartMS int64 `json:"media_window_start_ms"`
	// MediaWindowEndMS is the absolute media end time (after rebasing) in milliseconds.
	MediaWindowEndMS int64 `json:"media_window_end_ms"`
	// Timebase indicates the time reference (e.g., "audio_window_ms", "streamplace_running_time_ms").
	Timebase string `json:"timebase,omitempty"`
	// AudioWindowSeq is the sequence number of the audio window for rebasing.
	AudioWindowSeq int64 `json:"audio_window_seq"`
}

type AudioWindowAnchorUpdate struct {
	Type                string `json:"type"`
	AudioWindowSeq      int64  `json:"audio_window_seq"`
	MediaWindowStartMS  int64  `json:"media_window_start_ms"`
	MediaWindowDurMS    int64  `json:"media_window_dur_ms"`
	MediaClockTimebase  string `json:"media_clock_timebase"`
	StreamplaceStreamID string `json:"streamplace_stream_id,omitempty"`
}

func SendStreamUpdate(ctx context.Context, updateURL string, streamID string, pipeline string, updateData any) error {
	if updateURL == "" {
		return nil
	}
	if streamID == "" {
		return fmt.Errorf("missing streamID")
	}
	if pipeline == "" {
		return fmt.Errorf("missing pipeline")
	}

	// Mirror the Livepeer update header format used by the UI.
	env := envelope{
		Request:        mustJSON(map[string]any{"stream_id": streamID}),
		ParametersJSON: mustJSON(map[string]any{}),
		Capability:     pipeline,
		TimeoutSeconds: 5,
	}
	envBytes, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("marshal update envelope: %w", err)
	}
	livepeerHeader := base64.StdEncoding.EncodeToString(envBytes)

	b, err := json.Marshal(updateData)
	if err != nil {
		return fmt.Errorf("marshal update: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, updateURL, bytes.NewReader(b))
	if err != nil {
		return fmt.Errorf("new update request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Livepeer", livepeerHeader)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("post update: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodySize))
		return fmt.Errorf("update failed: %s: %s", resp.Status, string(body))
	}
	return nil
}


// Stats contains performance statistics for a transcription event.
type Stats struct {
	// AudioDurationMS is the duration of the audio window in milliseconds.
	AudioDurationMS int `json:"audio_duration_ms"`
}

// EventHandler is a callback function for processing transcript events.
type EventHandler func(ctx context.Context, event TranscriptEvent)

// ReadSSE connects to the SSE data stream and invokes handler for each transcript event.
// It blocks until the context is cancelled or the stream ends.
func ReadSSE(ctx context.Context, dataURL string, handler EventHandler) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, dataURL, nil)
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return fmt.Errorf("do request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, maxResponseBodySize))
		return fmt.Errorf("data stream failed: %s: %s", resp.Status, string(b))
	}

	log.Debug(ctx, "AI gateway SSE connected", "dataURL", dataURL)

	scanner := bufio.NewScanner(resp.Body)
	buf := make([]byte, 0, sseBufferSize)
	scanner.Buffer(buf, sseMaxBufferSize)

	var eventBuf strings.Builder

	flushEvent := func() {
		if eventBuf.Len() == 0 {
			return
		}
		data := strings.TrimSpace(eventBuf.String())
		if data == "" {
			eventBuf.Reset()
			return
		}
		events, err := parseSSEPayload(data)
		if err != nil {
			trunc := data
			if len(trunc) > 256 {
				trunc = trunc[:256] + "..."
			}
			log.Warn(ctx, "failed to parse SSE payload", "error", err, "data", trunc)
			eventBuf.Reset()
			return
		}

		for _, event := range events {
			event.ReceivedAt = time.Now()
			handler(ctx, event)
		}
		eventBuf.Reset()
	}

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			flushEvent()
			return ctx.Err()
		default:
		}

		line := scanner.Text()

		if strings.HasPrefix(line, "data:") {
			payload := strings.TrimSpace(line[len("data:"):])
			if eventBuf.Len() > 0 {
				eventBuf.WriteByte('\n')
			}
			eventBuf.WriteString(payload)
		} else if strings.TrimSpace(line) == "" {
			flushEvent()
		}
	}

	if err := scanner.Err(); err != nil && !isContextError(err, ctx) {
		return fmt.Errorf("scanner error: %w", err)
	}

	flushEvent()
	return nil
}

func parseSSEPayload(data string) ([]TranscriptEvent, error) {
	// Newer gateways may send:
	// - a single TranscriptEvent JSON object
	// - an array of TranscriptEvent objects
	// - a wrapper object containing an events array
	// Older gateways may send an array of JSON-encoded strings.
	var directMany []TranscriptEvent
	if err := json.Unmarshal([]byte(data), &directMany); err == nil {
		return directMany, nil
	}

	var wrapper struct {
		Events []TranscriptEvent `json:"events"`
	}
	if err := json.Unmarshal([]byte(data), &wrapper); err == nil && len(wrapper.Events) > 0 {
		return wrapper.Events, nil
	}

	var outer []string
	if err := json.Unmarshal([]byte(data), &outer); err != nil {
		var single TranscriptEvent
		if err2 := json.Unmarshal([]byte(data), &single); err2 == nil {
			return []TranscriptEvent{single}, nil
		}
		return nil, fmt.Errorf("unmarshal outer array: %w", err)
	}

	var events []TranscriptEvent
	for _, s := range outer {
		var event TranscriptEvent
		if err := json.Unmarshal([]byte(s), &event); err != nil {
			continue
		}
		events = append(events, event)
	}
	return events, nil
}

func isContextError(err error, ctx context.Context) bool {
	if err == nil {
		return false
	}
	if ctx.Err() != nil {
		return true
	}
	if strings.Contains(err.Error(), "context canceled") || strings.Contains(err.Error(), "use of closed network connection") {
		return true
	}
	return false
}
