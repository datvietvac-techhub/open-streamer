package buffer

import "github.com/datvietvac-techhub/open-streamer/internal/domain"

// RawIngestBufferID is the Buffer Hub slot where ingest writes MPEG-TS when a
// transcoder republishes to the main stream buffer. The prefix is not allowed
// in API stream codes (ValidateStreamCode), so it cannot collide with user codes.
func RawIngestBufferID(code domain.StreamCode) domain.StreamCode {
	return domain.StreamCode("$raw$" + string(code))
}
