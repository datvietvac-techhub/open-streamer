package coordinator

// deps.go — narrow interfaces that Coordinator uses for each collaborating service.
// Using interfaces instead of concrete types keeps the coordinator testable without
// starting real ingestors, transcoder subprocesses, or RTSP servers.

import (
	"context"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/manager"
	"github.com/datvietvac-techhub/open-streamer/internal/publisher"
	"github.com/datvietvac-techhub/open-streamer/internal/transcoder"
)

// mgrDep is the subset of manager.Service the coordinator needs.
type mgrDep interface {
	IsRegistered(domain.StreamCode) bool
	RegisteredStreams() []domain.StreamCode
	Register(context.Context, *domain.Stream, domain.StreamCode) error
	Unregister(domain.StreamCode)
	UpdateInputs(domain.StreamCode, []domain.Input, []domain.Input, []domain.Input)
	UpdateBufferWriteID(domain.StreamCode, domain.StreamCode)
	SetExhaustedCallback(func(domain.StreamCode))
	SetRestoredCallback(func(domain.StreamCode))
	RuntimeStatus(domain.StreamCode) (manager.RuntimeStatus, bool)
}

// tcDep is the subset of transcoder.Service the coordinator needs.
type tcDep interface {
	Start(context.Context, domain.StreamCode, domain.StreamCode, *domain.TranscoderConfig, []transcoder.RenditionTarget) error
	Stop(domain.StreamCode)
	StopProfile(domain.StreamCode, int)
	StartProfile(domain.StreamCode, int, transcoder.RenditionTarget) error
	SetUnhealthyCallback(func(streamID domain.StreamCode, reason string))
	SetHealthyCallback(func(streamID domain.StreamCode))
}

// pubDep is the subset of publisher.Service the coordinator needs.
type pubDep interface {
	Start(context.Context, *domain.Stream) error
	Stop(domain.StreamCode)
	UpdateProtocols(context.Context, *domain.Stream, *domain.Stream) error
	RestartHLSDASH(context.Context, *domain.Stream) error
	UpdateABRMasterMeta(domain.StreamCode, []publisher.ABRRepMeta)
}

// dvrDep is the subset of dvr.Service the coordinator needs.
type dvrDep interface {
	IsRecording(domain.StreamCode) bool
	StartRecording(context.Context, domain.StreamCode, domain.StreamCode, *domain.StreamDVRConfig) (*domain.Recording, error)
	StopRecording(context.Context, domain.StreamCode) error
}
