package aigateway

import (
	"fmt"
	"sort"
	"strings"
	"time"
)

const (
	// minCueDurationMS is the minimum duration for a VTT cue in milliseconds.
	minCueDurationMS = 1000
	// cueStartEpsilonMS avoids emitting cue start times of exactly 00:00:00.000.
	// Some HLS players behave poorly when cues begin at 0.
	cueStartEpsilonMS = 1
	// cueLeadInMS shifts cue start earlier to make subtitles appear sooner.
	// It is clamped to the segment start.
	cueLeadInMS = 250
	// cueLingerMS extends cue end times to improve readability and reduce flicker.
	// It is capped to the segment end.
	cueLingerMS = 500
)

// FormatVTTTime formats a millisecond timestamp as a VTT time string (HH:MM:SS.mmm).
func FormatVTTTime(ms int64) string {
	d := time.Duration(ms) * time.Millisecond
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60
	millis := int(d.Milliseconds()) % 1000
	return fmt.Sprintf("%02d:%02d:%02d.%03d", hours, minutes, seconds, millis)
}

func clampInt64(v, lo, hi int64) int64 {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

func maxInt64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// GenerateVTTForSegment generates a WebVTT file suitable for use as an HLS subtitle
// segment. Cue times are relative to the segment start (i.e., within the segment
// duration window), and only transcript segments that overlap the segment time
// window are included.
func GenerateVTTForSegment(segs []TranscriptSegment, segmentStartMS, segmentEndMS int64) []byte {
	if len(segs) == 0 {
		return []byte("WEBVTT\n\n")
	}
	if segmentEndMS <= segmentStartMS {
		return []byte("WEBVTT\n\n")
	}
	segmentDurMS := segmentEndMS - segmentStartMS

	// Ensure stable ordering even if producers resend or arrive out of order.
	ordered := make([]TranscriptSegment, 0, len(segs))
	for _, s := range segs {
		if strings.TrimSpace(s.Text) == "" {
			continue
		}
		if s.EndMS <= s.StartMS {
			continue
		}
		ordered = append(ordered, s)
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].StartMS == ordered[j].StartMS {
			return ordered[i].EndMS < ordered[j].EndMS
		}
		return ordered[i].StartMS < ordered[j].StartMS
	})

	var sb strings.Builder
	sb.WriteString("WEBVTT\n\n")

	windowed := make([]TranscriptSegment, 0, len(ordered))
	for _, seg := range ordered {
		// Include if overlapping [segmentStartMS, segmentEndMS)
		if seg.EndMS <= segmentStartMS || seg.StartMS >= segmentEndMS {
			continue
		}
		windowed = append(windowed, seg)
	}

	// If no overlapping transcripts found, check for recent transcripts that are close.
	// This handles the case where transcript pipeline lags behind HLS segments.
	const maxLagMS int64 = 5000 // Allow up to 5 seconds of lag
	if len(windowed) == 0 && len(ordered) > 0 {
		// Find the most recent transcript that ended before this segment
		var mostRecent *TranscriptSegment
		for i := len(ordered) - 1; i >= 0; i-- {
			if ordered[i].EndMS <= segmentStartMS && segmentStartMS-ordered[i].EndMS < maxLagMS {
				mostRecent = &ordered[i]
				break
			}
		}
		if mostRecent != nil {
			windowed = append(windowed, *mostRecent)
		}
	}

	stretchSingleCue := len(windowed) == 1

	for _, seg := range windowed {
		startAbs := maxInt64(seg.StartMS, segmentStartMS)
		endAbs := minInt64(seg.EndMS, segmentEndMS)
		if stretchSingleCue {
			// For a single cue, start from segment beginning and extend to segment end.
			// This handles timing misalignment between audio/transcript pipeline and HLS pipeline.
			startAbs = segmentStartMS
			endAbs = segmentEndMS
		}

		startMS := clampInt64((startAbs-segmentStartMS)-cueLeadInMS, 0, segmentDurMS)
		if startMS == 0 {
			startMS = clampInt64(cueStartEpsilonMS, 0, segmentDurMS)
		}
		endMS := clampInt64(endAbs-segmentStartMS, 0, segmentDurMS)
		// Extend the cue slightly past its end (up to the segment boundary) to reduce flicker.
		endMS = clampInt64(endMS+cueLingerMS, 0, segmentDurMS)
		if endMS < startMS+minCueDurationMS {
			endMS = clampInt64(startMS+minCueDurationMS, 0, segmentDurMS)
		}
		if endMS <= startMS {
			continue
		}

		cueText := strings.TrimSpace(seg.Text)
		if cueText == "" {
			continue
		}

		sb.WriteString(fmt.Sprintf("%s --> %s\n", FormatVTTTime(startMS), FormatVTTTime(endMS)))
		sb.WriteString(cueText)
		sb.WriteString("\n\n")
	}

	return []byte(sb.String())
}
