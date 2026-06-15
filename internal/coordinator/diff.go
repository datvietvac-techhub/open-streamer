package coordinator

import (
	"reflect"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
)

// StreamDiff describes what changed between two versions of a stream configuration.
// Only non-metadata fields are compared (name, description, tags, timestamps are ignored).
type StreamDiff struct {
	// Inputs
	InputsChanged bool
	AddedInputs   []domain.Input // priority exists in new but not old
	RemovedInputs []domain.Input // priority exists in old but not new
	UpdatedInputs []domain.Input // same priority, different config (carries NEW input)

	// Transcoder
	TranscoderChanged         bool          // any transcoder config change
	TranscoderTopologyChanged bool          // nil↔non-nil, mode change, video.copy change → buffer layout changes
	ProfilesDiff              *ProfilesDiff // per-profile granular diff; nil when topology changed

	// Publisher
	ProtocolsChanged bool
	PushChanged      bool

	// PlaybackPolicyChanged is true when the bound media-auth policy code
	// changed. The publisher mirrors the binding in-memory (streamState.
	// playbackPolicy) for O(1) authorization lookups, so a live stream must be
	// told to refresh it — otherwise the new policy only takes effect on the
	// next restart and playback keeps using the stale binding.
	PlaybackPolicyChanged bool

	// DVR
	DVRChanged bool

	// Lifecycle
	NowDisabled bool // was enabled → now disabled
}

// ProfilesDiff holds per-profile changes when the transcoder topology stays the same.
type ProfilesDiff struct {
	Added   []ProfileChange
	Removed []ProfileChange
	Updated []ProfileChange
}

// ProfileChange describes one profile that was added, removed, or updated.
type ProfileChange struct {
	Index int
	Old   *domain.VideoProfile // nil when added
	New   *domain.VideoProfile // nil when removed
}

// HasProfileChanges reports whether there are any per-profile changes.
func (d *ProfilesDiff) HasProfileChanges() bool {
	return d != nil && (len(d.Added) > 0 || len(d.Removed) > 0 || len(d.Updated) > 0)
}

// HasAddedOrRemoved reports whether the profile count changed (master playlist rebuild needed).
func (d *ProfilesDiff) HasAddedOrRemoved() bool {
	return d != nil && (len(d.Added) > 0 || len(d.Removed) > 0)
}

// ComputeDiff compares two stream configurations and returns a diff.
func ComputeDiff(old, new *domain.Stream) StreamDiff {
	var d StreamDiff

	d.NowDisabled = !old.Disabled && new.Disabled

	diffInputs(old, new, &d)
	diffTranscoder(old, new, &d)
	d.ProtocolsChanged = !protocolsEqual(old.Protocols, new.Protocols)
	d.PushChanged = !pushSliceEqual(old.Push, new.Push)
	d.PlaybackPolicyChanged = old.PlaybackPolicy != new.PlaybackPolicy
	d.DVRChanged = !dvrConfigEqual(old.DVR, new.DVR)

	return d
}

// diffInputs compares inputs by priority as the stable key.
func diffInputs(old, new *domain.Stream, d *StreamDiff) {
	oldMap := make(map[int]domain.Input, len(old.Inputs))
	for _, inp := range old.Inputs {
		oldMap[inp.Priority] = inp
	}
	newMap := make(map[int]domain.Input, len(new.Inputs))
	for _, inp := range new.Inputs {
		newMap[inp.Priority] = inp
	}

	for p, ni := range newMap {
		oi, exists := oldMap[p]
		if !exists {
			d.AddedInputs = append(d.AddedInputs, ni)
			continue
		}
		if !inputsEqual(oi, ni) {
			d.UpdatedInputs = append(d.UpdatedInputs, ni)
		}
	}
	for p, oi := range oldMap {
		if _, exists := newMap[p]; !exists {
			d.RemovedInputs = append(d.RemovedInputs, oi)
		}
	}

	d.InputsChanged = len(d.AddedInputs) > 0 || len(d.RemovedInputs) > 0 || len(d.UpdatedInputs) > 0
}

// diffTranscoder compares transcoder configs with two granularity levels:
//  1. Topology change (buffer layout changes OR watermark filter graph change)
//     → full restart needed
//  2. Per-profile diff (only specific renditions need restart).
//
// Watermark belongs in this function because the filter graph it produces
// lives in the encoder's libavfilter chain — there's no way to swap it
// without restarting the encoder. Coordinator's `transcoderConfigWithWatermark`
// rebuilds the runtime config on each `tc.Start`, so a topology reload is
// the cheapest reliable path that picks up the new watermark.
func diffTranscoder(old, new *domain.Stream, d *StreamDiff) {
	transcoderEq := reflect.DeepEqual(old.Transcoder, new.Transcoder)
	watermarkEq := reflect.DeepEqual(old.Watermark, new.Watermark)
	if transcoderEq && watermarkEq {
		return
	}

	// Watermark-only change: route to topology reload when FFmpeg is
	// actually running on either side. Passthrough streams (no FFmpeg)
	// never see the filter graph so the change is moot.
	if transcoderEq && !watermarkEq {
		if needsFFmpeg(old.Transcoder) || needsFFmpeg(new.Transcoder) {
			d.TranscoderChanged = true
			d.TranscoderTopologyChanged = true
		}
		return
	}

	d.TranscoderChanged = true

	// nil ↔ non-nil is a topology change.
	if (old.Transcoder == nil) != (new.Transcoder == nil) {
		d.TranscoderTopologyChanged = true
		return
	}

	// Both non-nil from here.
	ot, nt := old.Transcoder, new.Transcoder

	// FFmpeg on/off change is a topology change (buffer layout changes).
	if needsFFmpeg(ot) != needsFFmpeg(nt) {
		d.TranscoderTopologyChanged = true
		return
	}

	// video.copy change is a topology change (single passthrough rendition ↔ multi-profile ladder).
	if ot.Video.Copy != nt.Video.Copy {
		d.TranscoderTopologyChanged = true
		return
	}

	// Mode-change branch removed alongside TranscoderConfig.Mode in the
	// native-transcoder migration (see internal/transcoder/native).
	// The native pipeline has one topology — nothing left to switch
	// between, so no phantom restart can be triggered here.

	// If neither config needs the transcoder, there are no profiles to diff.
	if !needsFFmpeg(nt) {
		return
	}

	// Watermark change alongside transcoder change still forces topology
	// reload — cheaper than trying to compose a per-profile path that
	// would need a sw.tc refresh transcoder doesn't currently expose.
	if !watermarkEq {
		d.TranscoderTopologyChanged = true
		return
	}

	// Global, audio, decoder change → all profiles must restart.
	// ExtraArgs check dropped with the rest of FFmpeg-CLI plumbing.
	globalChanged := ot.Global != nt.Global ||
		!reflect.DeepEqual(ot.Audio, nt.Audio) ||
		ot.Decoder != nt.Decoder

	pd := diffProfiles(ot.Video.Profiles, nt.Video.Profiles, globalChanged)
	d.ProfilesDiff = pd
}

// diffProfiles compares video profile slices by index.
func diffProfiles(oldProfiles, newProfiles []domain.VideoProfile, allChanged bool) *ProfilesDiff {
	pd := &ProfilesDiff{}

	maxLen := len(oldProfiles)
	if len(newProfiles) > maxLen {
		maxLen = len(newProfiles)
	}

	for i := range maxLen {
		haveOld := i < len(oldProfiles)
		haveNew := i < len(newProfiles)

		switch {
		case haveOld && haveNew:
			if allChanged || oldProfiles[i] != newProfiles[i] {
				op := oldProfiles[i]
				np := newProfiles[i]
				pd.Updated = append(pd.Updated, ProfileChange{Index: i, Old: &op, New: &np})
			}
		case haveOld && !haveNew:
			op := oldProfiles[i]
			pd.Removed = append(pd.Removed, ProfileChange{Index: i, Old: &op})
		case !haveOld && haveNew:
			np := newProfiles[i]
			pd.Added = append(pd.Added, ProfileChange{Index: i, New: &np})
		}
	}

	return pd
}

// needsFFmpeg reports whether the transcoder config requires running the
// native pipeline. Name kept for grep continuity across the FFmpeg →
// native migration; semantics unchanged (both video and audio copy means
// raw MPEG-TS can pass through without re-encoding).
func needsFFmpeg(tc *domain.TranscoderConfig) bool {
	if tc == nil {
		return false
	}
	return !tc.Video.Copy || !tc.Audio.Copy
}

func inputsEqual(a, b domain.Input) bool {
	return a.URL == b.URL &&
		reflect.DeepEqual(a.Headers, b.Headers) &&
		reflect.DeepEqual(a.Params, b.Params) &&
		a.Net == b.Net
}

func pushSliceEqual(a, b []domain.PushDestination) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i].URL != b[i].URL ||
			a[i].Enabled != b[i].Enabled ||
			a[i].TimeoutSec != b[i].TimeoutSec ||
			a[i].RetryTimeoutSec != b[i].RetryTimeoutSec ||
			a[i].Limit != b[i].Limit {
			return false
		}
	}
	return true
}

func dvrConfigEqual(a, b *domain.StreamDVRConfig) bool {
	if a == nil && b == nil {
		return true
	}
	if (a == nil) != (b == nil) {
		return false
	}
	return *a == *b
}

// protocolsEqual treats two nil pointers as equal (both "unconfigured") and
// otherwise compares the dereferenced struct values. A nil pointer is NOT
// equal to a non-nil pointer pointing at the zero value — the operator
// asserted "no protocols enabled" explicitly, which is a different intent
// from "inherit / leave alone" and should trigger a protocol restart.
func protocolsEqual(a, b *domain.OutputProtocols) bool {
	if a == nil && b == nil {
		return true
	}
	if (a == nil) != (b == nil) {
		return false
	}
	return *a == *b
}
