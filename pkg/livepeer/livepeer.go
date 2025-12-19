package livepeer

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"mime"
	"mime/multipart"
	"net/http"
	"os"
	"strings"
	"time"

	"stream.place/streamplace/pkg/aqhttp"
	"stream.place/streamplace/pkg/config"
	"stream.place/streamplace/pkg/log"
	"stream.place/streamplace/pkg/media"
	"stream.place/streamplace/pkg/renditions"
	"stream.place/streamplace/pkg/spmetrics"
	"stream.place/streamplace/pkg/streamplace"
)

const SegmentsInFlight = 2

type StreamUrls struct {
	StreamID      string `json:"stream_id"`
	WhipURL       string `json:"whip_url"`
	WhepURL       string `json:"whep_url"`
	RtmpURL       string `json:"rtmp_url"`
	RtmpOutputURL string `json:"rtmp_output_url"`
	UpdateURL     string `json:"update_url"`
	StatusURL     string `json:"status_url"`
	DataURL       string `json:"data_url"`
	StopURL       string `json:"stop_url"`
}

type LivepeerSession struct {
	SessionID  string
	Count      int
	GatewayURL string
	Guard      chan struct{}
	CLI        *config.CLI
}

// borrowed from catalyst-api
func RandomTrailer(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyz0123456789"

	res := make([]byte, length)
	for i := 0; i < length; i++ {
		res[i] = charset[rand.Intn(len(charset))]
	}
	return string(res)
}

func NewLivepeerSession(ctx context.Context, cli *config.CLI, did string, gatewayURL string) (*LivepeerSession, error) {
	sessionID := fmt.Sprintf("%s-%s", did, RandomTrailer(8))
	sessionID = strings.ReplaceAll(sessionID, ":", "")
	sessionID = strings.ReplaceAll(sessionID, ".", "")
	return &LivepeerSession{
		SessionID:  sessionID,
		Count:      0,
		GatewayURL: gatewayURL,
		Guard:      make(chan struct{}, SegmentsInFlight),
		CLI:        cli,
	}, nil
}

func (ls *LivepeerSession) PostSegmentToGateway(ctx context.Context, buf []byte, spseg *streamplace.Segment, rs renditions.Renditions) ([][]byte, *StreamUrls, error) {
	ctx = log.WithLogValues(ctx, "func", "PostSegmentToGateway")
	lpProfiles := rs.ToLivepeerProfiles()
	sessionIDRen := fmt.Sprintf("%s-%dren", ls.SessionID, len(rs))
	transcodingConfiguration := map[string]any{
		"manifestID": sessionIDRen,
		"profiles":   lpProfiles,
	}

	vid := spseg.Video[0]
	ingestWidth := int(vid.Width)
	ingestHeight := int(vid.Height)

	width := 512
	height := 512

	if ls.CLI.LivepeerAIProcessing {
		aiJobSettings := map[string]any{
			"enable_video_ingress": ls.CLI.LivepeerAIEnableVideoIngress,
			"enable_audio_ingress": ls.CLI.LivepeerAIEnableAudioIngress,
			"enable_video_egress":  ls.CLI.LivepeerAIEnableVideoEgress,
			"enable_audio_egress":  ls.CLI.LivepeerAIEnableAudioEgress,
			"enable_data_output":   ls.CLI.LivepeerAIEnableDataOutput,
		}

		aiJobSettingsJSON, err := json.Marshal(aiJobSettings)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to marshal AI job params: %w", err)
		}
		aiJobSettingsStr := string(aiJobSettingsJSON)

		// Read audio transcription API JSON file
		promptsJSONBytes, err := os.ReadFile("audio-transcription-api.json")
		if err != nil {
			return nil, nil, fmt.Errorf("failed to read audio-transcription-api.json: %w", err)
		}
		var promptsContent map[string]any
		err = json.Unmarshal(promptsJSONBytes, &promptsContent)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to parse audio-transcription-api.json: %w", err)
		}

		promptsJSONString, err := json.MarshalIndent(promptsContent, "", "  ")
		if err != nil {
			return nil, nil, fmt.Errorf("failed to marshal prompts: %w", err)
		}

		aiJobParams := map[string]any{
			"height":  height,
			"prompts": string(promptsJSONString),
			"width":   width,
		}

		aiJobParamsStr, err := json.Marshal(aiJobParams)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to marshal AI job params: %w", err)
		}

		log.Debug(ctx, "ai job params", "aiJobParams", aiJobParams)

		transcodingConfiguration["aiParams"] = map[string]any{
			"capability":      ls.CLI.LivepeerAICapability,
			"parameters":      string(aiJobSettingsStr),
			"request":         "{}",
			"timeout_seconds": 60,
			"stream_id":       sessionIDRen,
			"params":          string(aiJobParamsStr),
		}

		if ls.CLI.LivepeerAIStreamKey != "" {
			transcodingConfiguration["streamKey"] = ls.CLI.LivepeerAIStreamKey
		}
	}

	log.Debug(ctx, "transcoding configuration", "transcodingConfiguration", transcodingConfiguration)

	bs, err := json.Marshal(transcodingConfiguration)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal livepeer profile: %w", err)
	}
	tsSeg := bytes.Buffer{}
	audioSeg := bytes.Buffer{}
	err = media.MP4ToMPEGTSVideoMP4Audio(ctx, bytes.NewReader(buf), &tsSeg, &audioSeg)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to convert mp4 to ts video/mp4 audio: %w", err)
	}
	if tsSeg.Len() == 0 {
		return nil, nil, fmt.Errorf("no video in segment")
	}
	if audioSeg.Len() == 0 {
		return nil, nil, fmt.Errorf("no audio in segment")
	}
	ls.Guard <- struct{}{}
	start := time.Now()
	// check if context is done since we were waiting for the lock
	if ctx.Err() != nil {
		<-ls.Guard
		return nil, nil, ctx.Err()
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	seqNo := ls.Count

	//# TODO: do a replace /live with live2 if ai processing is enabled
	url := fmt.Sprintf("%s/live2/%s/%d.ts", ls.GatewayURL, sessionIDRen, seqNo)
	ls.Count++

	dur := time.Duration(*spseg.Duration)
	durationMs := int(dur.Milliseconds())
	log.Debug(ctx, "posting segment to livepeer gateway", "duration_ms", durationMs, "url", url)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(tsSeg.Bytes()))
	if err != nil {
		<-ls.Guard
		return nil, nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Accept", "multipart/mixed")
	req.Header.Set("Content-Duration", fmt.Sprintf("%d", durationMs))
	req.Header.Set("Content-Resolution", fmt.Sprintf("%dx%d", ingestWidth, ingestHeight))
	req.Header.Set("Livepeer-Transcode-Configuration", string(bs))

	if ls.CLI.LivepeerDebug {
		debugDir := ls.CLI.DataFilePath([]string{"livepeer-debug"})
		err = os.MkdirAll(debugDir, 0755)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to create debug directory: %w", err)
		}
		debugFile := fmt.Sprintf("%s/livepeer-debug/%s-%06d-input.ts", ls.CLI.DataDir, sessionIDRen, seqNo)
		err = os.WriteFile(debugFile, tsSeg.Bytes(), 0644)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to write debug file: %w", err)
		}
		bs, err := json.MarshalIndent(req.Header, "", "  ")
		if err != nil {
			return nil, nil, fmt.Errorf("failed to marshal livepeer profile: %w", err)
		}
		configFile := fmt.Sprintf("%s/livepeer-debug/%s-%06d-config.json", ls.CLI.DataDir, sessionIDRen, seqNo)
		err = os.WriteFile(configFile, bs, 0644)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to write debug file: %w", err)
		}
		log.Log(ctx, "wrote debug file", "file", debugFile)
	}

	resp, err := aqhttp.DoTrusted(ctx, req)
	if err != nil {
		<-ls.Guard
		return nil, nil, fmt.Errorf("failed to send segment to gateway (config %s): %w", string(bs), err)
	}
	<-ls.Guard
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		errOut, _ := io.ReadAll(resp.Body)
		return nil, nil, fmt.Errorf("gateway returned non-OK status (config %s): %d, %s", string(bs), resp.StatusCode, string(errOut))
	}

	var streamUrls *StreamUrls
	if aiURLsB64 := resp.Header.Get("X-AI-Stream-Urls"); aiURLsB64 != "" {
		decoded, err := base64.StdEncoding.DecodeString(aiURLsB64)
		if err != nil {
			log.Error(ctx, "failed to decode X-AI-Stream-Urls", "error", err)
		} else {
			log.Log(ctx, "received ai stream urls from gateway", "urls", string(decoded))
			var urls StreamUrls
			if err := json.Unmarshal(decoded, &urls); err != nil {
				log.Error(ctx, "failed to parse ai stream urls", "error", err)
			} else {
				streamUrls = &urls
				log.Log(ctx, "parsed ai stream urls", "data_url", urls.DataURL)
			}
		}
	}

	var out [][]byte

	mediaType, params, err := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to parse media type: %w", err)
	}
	if strings.HasPrefix(mediaType, "multipart/") {
		mr := multipart.NewReader(resp.Body, params["boundary"])
		for {
			p, err := mr.NextPart()
			if err == io.EOF {
				break
			}
			ctx := log.WithLogValues(ctx, "part", p.FileName())
			if err != nil {
				return nil, nil, fmt.Errorf("failed to get next part: %w", err)
			}

			// Detect and log AI data/text outputs instead of trying to transcode them.
			contentType := p.Header.Get("Content-Type")
			if strings.HasPrefix(contentType, "application/json") || strings.HasPrefix(contentType, "text/") {
				body, readErr := io.ReadAll(p)
				if readErr != nil {
					log.Error(ctx, "failed to read ai data output", "error", readErr)
				} else {
					log.Log(ctx, "received ai data output", "contentType", contentType, "length", len(body), "body", string(body))
				}
				continue
			}

			mp4Bs := bytes.Buffer{}
			audioReader := bytes.NewReader(audioSeg.Bytes())
			if ls.CLI.LivepeerDebug {
				debugFile := fmt.Sprintf("%s/livepeer-debug/%s-%06d-output-%s", ls.CLI.DataDir, sessionIDRen, seqNo, p.FileName())
				err = os.WriteFile(debugFile, tsSeg.Bytes(), 0644)
				if err != nil {
					return nil, nil, fmt.Errorf("failed to write debug file: %w", err)
				}
				log.Log(ctx, "wrote debug file", "file", debugFile)
			}
			err = media.MPEGTSVideoMP4AudioToMP4(ctx, p, audioReader, &mp4Bs)
			if err != nil {
				return nil, nil, fmt.Errorf("failed to convert ts to mp4: %w", err)
			}
			bs := mp4Bs.Bytes()
			log.Debug(ctx, "got part back from livepeer gateway", "length", len(bs), "name", p.FileName())
			out = append(out, bs)
		}
	}
	spmetrics.TranscodeDuration.WithLabelValues(spseg.Creator).Observe(float64(time.Since(start).Milliseconds()))
	return out, streamUrls, nil
}
