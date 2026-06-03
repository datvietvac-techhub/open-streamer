package transcoder

import "github.com/datvietvac-techhub/open-streamer/internal/domain"

// RenditionTarget binds one encoded ladder rung to its Buffer Hub output slot.
type RenditionTarget struct {
	BufferID domain.StreamCode
	Profile  Profile
}
