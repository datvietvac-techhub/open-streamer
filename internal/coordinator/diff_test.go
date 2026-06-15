package coordinator

import (
	"testing"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/stretchr/testify/assert"
)

func baseStream() *domain.Stream {
	return &domain.Stream{
		Code: "test",
		Name: "Test Stream",
		Inputs: []domain.Input{
			{URL: "http://a.m3u8", Priority: 0},
			{URL: "http://b.m3u8", Priority: 1},
		},
		Protocols: &domain.OutputProtocols{HLS: true},
		Transcoder: &domain.TranscoderConfig{
			Video: domain.VideoTranscodeConfig{
				Profiles: []domain.VideoProfile{
					{Width: 1920, Height: 1080, Bitrate: 5000, Codec: "h264"},
					{Width: 640, Height: 360, Bitrate: 800, Codec: "h264"},
				},
			},
		},
	}
}

// passthroughStream returns a baseStream variant with both video and audio
// in copy mode so needsFFmpeg returns false. Used by watermark tests that
// need to verify the "no FFmpeg ever runs → watermark change is moot" path.
func passthroughStream() *domain.Stream {
	s := baseStream()
	s.Transcoder = &domain.TranscoderConfig{
		Video: domain.VideoTranscodeConfig{Copy: true},
		Audio: domain.AudioTranscodeConfig{Copy: true},
	}
	return s
}

// Transcoder topology toggle test removed alongside TranscoderConfig.Mode
// in the native-transcoder migration. The native pipeline has a single
// topology; nothing for the diff engine to switch between.

func TestComputeDiff_WatermarkAddedTriggersTopology(t *testing.T) {
	old := baseStream()
	new := baseStream()
	new.Watermark = &domain.WatermarkConfig{
		Enabled: true, Type: domain.WatermarkTypeText, Text: "LIVE",
	}

	d := ComputeDiff(old, new)
	// Filter graph baked into FFmpeg argv → must restart to pick up.
	assert.True(t, d.TranscoderChanged)
	assert.True(t, d.TranscoderTopologyChanged)
}

func TestComputeDiff_WatermarkRemovedTriggersTopology(t *testing.T) {
	old := baseStream()
	old.Watermark = &domain.WatermarkConfig{
		Enabled: true, Type: domain.WatermarkTypeText, Text: "LIVE",
	}
	new := baseStream()

	d := ComputeDiff(old, new)
	assert.True(t, d.TranscoderChanged)
	assert.True(t, d.TranscoderTopologyChanged)
}

func TestComputeDiff_WatermarkChangedAlongsideProfileForcesTopology(t *testing.T) {
	old := baseStream()
	old.Watermark = &domain.WatermarkConfig{
		Enabled: true, Type: domain.WatermarkTypeText, Text: "LIVE",
	}
	new := baseStream()
	new.Watermark = &domain.WatermarkConfig{
		Enabled: true, Type: domain.WatermarkTypeText, Text: "ON AIR",
	}
	new.Transcoder.Video.Profiles[0].Bitrate = 4000 // also bump bitrate

	d := ComputeDiff(old, new)
	assert.True(t, d.TranscoderChanged)
	// Watermark forces topology even when profiles also changed — per-profile
	// path can't refresh sw.tc cleanly.
	assert.True(t, d.TranscoderTopologyChanged)
}

func TestComputeDiff_WatermarkOnPassthroughIsNoop(t *testing.T) {
	old := passthroughStream()
	new := passthroughStream()
	new.Watermark = &domain.WatermarkConfig{
		Enabled: true, Type: domain.WatermarkTypeText, Text: "LIVE",
	}

	d := ComputeDiff(old, new)
	// Both ends are passthrough — no FFmpeg → watermark is moot.
	assert.False(t, d.TranscoderChanged, "watermark on passthrough should not trigger restart")
	assert.False(t, d.TranscoderTopologyChanged)
}

func TestComputeDiff_MetadataOnly(t *testing.T) {
	old := baseStream()
	new := baseStream()
	new.Name = "Renamed"
	new.Description = "New description"
	new.Tags = []string{"live"}

	d := ComputeDiff(old, new)

	assert.False(t, d.InputsChanged)
	assert.False(t, d.TranscoderChanged)
	assert.False(t, d.ProtocolsChanged)
	assert.False(t, d.PushChanged)
	assert.False(t, d.DVRChanged)
	assert.False(t, d.NowDisabled)
}

func TestComputeDiff_PlaybackPolicyOnly(t *testing.T) {
	old := baseStream()
	new := baseStream()
	new.PlaybackPolicy = "only_us"

	d := ComputeDiff(old, new)

	assert.True(t, d.PlaybackPolicyChanged, "policy code change must be detected")
	// A policy-only change must NOT look like a protocol/push/transcoder change.
	assert.False(t, d.ProtocolsChanged)
	assert.False(t, d.PushChanged)
	assert.False(t, d.TranscoderChanged)
	assert.False(t, d.DVRChanged)
	assert.False(t, d.InputsChanged)
}

func TestComputeDiff_PlaybackPolicyUnchanged(t *testing.T) {
	old := baseStream()
	old.PlaybackPolicy = "p1"
	new := baseStream()
	new.PlaybackPolicy = "p1"

	d := ComputeDiff(old, new)
	assert.False(t, d.PlaybackPolicyChanged)
}

func TestComputeDiff_InputAdded(t *testing.T) {
	old := baseStream()
	new := baseStream()
	new.Inputs = append(new.Inputs, domain.Input{URL: "http://c.m3u8", Priority: 2})

	d := ComputeDiff(old, new)

	assert.True(t, d.InputsChanged)
	assert.Len(t, d.AddedInputs, 1)
	assert.Equal(t, 2, d.AddedInputs[0].Priority)
	assert.Empty(t, d.RemovedInputs)
	assert.Empty(t, d.UpdatedInputs)
}

func TestComputeDiff_InputRemoved(t *testing.T) {
	old := baseStream()
	new := baseStream()
	new.Inputs = new.Inputs[:1] // keep only priority 0

	d := ComputeDiff(old, new)

	assert.True(t, d.InputsChanged)
	assert.Len(t, d.RemovedInputs, 1)
	assert.Equal(t, 1, d.RemovedInputs[0].Priority)
	assert.Empty(t, d.AddedInputs)
}

func TestComputeDiff_InputUpdated(t *testing.T) {
	old := baseStream()
	new := baseStream()
	new.Inputs[1].URL = "http://new-b.m3u8"

	d := ComputeDiff(old, new)

	assert.True(t, d.InputsChanged)
	assert.Len(t, d.UpdatedInputs, 1)
	assert.Equal(t, 1, d.UpdatedInputs[0].Priority)
	assert.Equal(t, "http://new-b.m3u8", d.UpdatedInputs[0].URL)
	assert.Empty(t, d.AddedInputs)
	assert.Empty(t, d.RemovedInputs)
}

func TestComputeDiff_TranscoderNilToNonNil(t *testing.T) {
	old := baseStream()
	old.Transcoder = nil
	new := baseStream()

	d := ComputeDiff(old, new)

	assert.True(t, d.TranscoderChanged)
	assert.True(t, d.TranscoderTopologyChanged)
	assert.Nil(t, d.ProfilesDiff)
}

func TestComputeDiff_TranscoderNonNilToNil(t *testing.T) {
	old := baseStream()
	new := baseStream()
	new.Transcoder = nil

	d := ComputeDiff(old, new)

	assert.True(t, d.TranscoderChanged)
	assert.True(t, d.TranscoderTopologyChanged)
}

func TestComputeDiffBothCopyEnabledTopologyChanged(t *testing.T) {
	// Enabling both Video.Copy + Audio.Copy turns FFmpeg off → topology change.
	old := baseStream()
	new := baseStream()
	new.Transcoder.Video.Copy = true
	new.Transcoder.Audio.Copy = true

	d := ComputeDiff(old, new)

	assert.True(t, d.TranscoderChanged)
	assert.True(t, d.TranscoderTopologyChanged)
}

func TestComputeDiff_ProfileUpdated(t *testing.T) {
	old := baseStream()
	new := baseStream()
	// Change track_2 from SD (640x360) to LD (320x180)
	new.Transcoder.Video.Profiles[1] = domain.VideoProfile{
		Width: 320, Height: 180, Bitrate: 400, Codec: "h264",
	}

	d := ComputeDiff(old, new)

	assert.True(t, d.TranscoderChanged)
	assert.False(t, d.TranscoderTopologyChanged)
	assert.NotNil(t, d.ProfilesDiff)
	assert.Empty(t, d.ProfilesDiff.Added)
	assert.Empty(t, d.ProfilesDiff.Removed)
	assert.Len(t, d.ProfilesDiff.Updated, 1)
	assert.Equal(t, 1, d.ProfilesDiff.Updated[0].Index)
	assert.Equal(t, 360, d.ProfilesDiff.Updated[0].Old.Height)
	assert.Equal(t, 180, d.ProfilesDiff.Updated[0].New.Height)
}

func TestComputeDiff_ProfileAdded(t *testing.T) {
	old := baseStream()
	new := baseStream()
	new.Transcoder.Video.Profiles = append(new.Transcoder.Video.Profiles,
		domain.VideoProfile{Width: 320, Height: 180, Bitrate: 400, Codec: "h264"},
	)

	d := ComputeDiff(old, new)

	assert.True(t, d.TranscoderChanged)
	assert.False(t, d.TranscoderTopologyChanged)
	assert.Len(t, d.ProfilesDiff.Added, 1)
	assert.Equal(t, 2, d.ProfilesDiff.Added[0].Index)
}

func TestComputeDiff_ProfileRemoved(t *testing.T) {
	old := baseStream()
	new := baseStream()
	new.Transcoder.Video.Profiles = new.Transcoder.Video.Profiles[:1] // keep only FHD

	d := ComputeDiff(old, new)

	assert.True(t, d.TranscoderChanged)
	assert.False(t, d.TranscoderTopologyChanged)
	assert.Len(t, d.ProfilesDiff.Removed, 1)
	assert.Equal(t, 1, d.ProfilesDiff.Removed[0].Index)
}

func TestComputeDiff_GlobalConfigChange_AllProfilesAffected(t *testing.T) {
	old := baseStream()
	new := baseStream()
	new.Transcoder.Global.HW = domain.HWAccelNVENC

	d := ComputeDiff(old, new)

	assert.True(t, d.TranscoderChanged)
	assert.False(t, d.TranscoderTopologyChanged)
	// All profiles marked as updated because global config affects FFmpeg args.
	assert.Len(t, d.ProfilesDiff.Updated, 2)
}

func TestComputeDiff_AudioChange_AllProfilesAffected(t *testing.T) {
	old := baseStream()
	new := baseStream()
	new.Transcoder.Audio.Codec = domain.AudioCodecMP3

	d := ComputeDiff(old, new)

	assert.True(t, d.TranscoderChanged)
	assert.False(t, d.TranscoderTopologyChanged)
	assert.Len(t, d.ProfilesDiff.Updated, 2)
}

func TestComputeDiff_ProtocolsChanged(t *testing.T) {
	old := baseStream()
	new := baseStream()
	new.Protocols.DASH = true

	d := ComputeDiff(old, new)

	assert.True(t, d.ProtocolsChanged)
	assert.False(t, d.TranscoderChanged)
}

func TestComputeDiff_PushChanged(t *testing.T) {
	old := baseStream()
	new := baseStream()
	new.Push = []domain.PushDestination{
		{URL: "rtmp://platform.example.com/live/key", Enabled: true},
	}

	d := ComputeDiff(old, new)

	assert.True(t, d.PushChanged)
}

func TestComputeDiff_DVRChanged(t *testing.T) {
	old := baseStream()
	new := baseStream()
	new.DVR = &domain.StreamDVRConfig{Enabled: true, RetentionSec: 3600}

	d := ComputeDiff(old, new)

	assert.True(t, d.DVRChanged)
}

func TestComputeDiff_NowDisabled(t *testing.T) {
	old := baseStream()
	new := baseStream()
	new.Disabled = true

	d := ComputeDiff(old, new)

	assert.True(t, d.NowDisabled)
}

func TestComputeDiff_VideoCopyChange_TopologyChanged(t *testing.T) {
	old := baseStream()
	new := baseStream()
	new.Transcoder.Video.Copy = true

	d := ComputeDiff(old, new)

	assert.True(t, d.TranscoderChanged)
	assert.True(t, d.TranscoderTopologyChanged)
}

func TestComputeDiff_NoChanges(t *testing.T) {
	old := baseStream()
	new := baseStream()

	d := ComputeDiff(old, new)

	assert.False(t, d.InputsChanged)
	assert.False(t, d.TranscoderChanged)
	assert.False(t, d.ProtocolsChanged)
	assert.False(t, d.PushChanged)
	assert.False(t, d.DVRChanged)
	assert.False(t, d.NowDisabled)
}
