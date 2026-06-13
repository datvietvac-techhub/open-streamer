package dash

import (
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// abr.go — coordinator for ABR (multi-rendition) DASH streams.
//
// Architecture:
//
//   per-rendition Packager (shard) ──┐
//   per-rendition Packager (shard) ──┼─► ABRMaster.UpdateShard(snap)
//   per-rendition Packager (shard) ──┘            │
//                                                 ▼
//                                       debounced root MPD write
//                                       (combines all shards into one
//                                        MPD with N <Representation>s)
//
// Each per-rendition Packager writes ITS OWN media segments to its
// shard directory. The ABRMaster does NOT touch media files — it only
// writes the root MPD at the stream's top-level directory, advertising
// all renditions as <Representation>s within one video AdaptationSet.
//
// Concurrency:
//
//   - UpdateShard is called by per-shard goroutines under their own
//     packager mutex. It MUST NOT block on the packager — fast path,
//     no I/O, just stash the snapshot and schedule a debounced flush.
//   - flushRoot runs on a debounce timer goroutine. It does the I/O
//     (XML build + atomic file write). Holds m.mu only while copying
//     out the shard map; XML build and write happen outside the lock.

// ABRMaster aggregates per-rendition snapshots and writes the root MPD.
type ABRMaster struct {
	rootManifest string
	streamID     string // for log fields
	segDur       time.Duration
	window       int

	mu        sync.Mutex
	shards    map[string]ShardSnapshot // slug → latest snapshot
	sharedAST time.Time                // ladder-wide availabilityStart, set once (B-4)
	debounce  *time.Timer
	stopped   bool
}

// SharedAST returns the ladder-wide availabilityStartTime, setting it to
// `candidate` on the first call with a non-zero candidate and returning that
// same value on every later call. Every shard anchors its timeline + pacing to
// this ONE value so the published AST is correct for all renditions, every
// shard's behindPrevSegEnd paces against the same origin, and the dynamic-MPD
// AST never changes mid-stream regardless of which shard flushes first (B-4).
//
// Safe for concurrent use; called from each shard's packager under that
// shard's mutex — like UpdateShard it only touches m's own state, so there is
// no cross-lock deadlock.
func (m *ABRMaster) SharedAST(candidate time.Time) time.Time {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.sharedAST.IsZero() && !candidate.IsZero() {
		m.sharedAST = candidate
	}
	return m.sharedAST
}

// ShardSnapshot is a point-in-time view of one rendition, produced by
// the per-shard Packager and passed to ABRMaster.UpdateShard.
//
// The slug ("track_1", "track_2", …) identifies the rendition within
// the stream and is used as the map key in ABRMaster.
type ShardSnapshot struct {
	Slug              string
	AvailabilityStart time.Time
	Video             *TrackManifest // nil if this shard has no video init yet
	Audio             *TrackManifest // only the primary shard packs audio
}

// NewABRMaster constructs a master that will write its root MPD to
// rootManifest. segDur and window are used for MPD duration attributes.
func NewABRMaster(rootManifest, streamID string, segDur time.Duration, window int) *ABRMaster {
	return &ABRMaster{
		rootManifest: rootManifest,
		streamID:     streamID,
		segDur:       segDur,
		window:       window,
		shards:       make(map[string]ShardSnapshot),
	}
}

// UpdateShard records the latest snapshot for one shard. Cheap and
// non-blocking — schedules a debounced flushRoot to actually write the
// MPD.
//
// The 150 ms debounce coalesces the typical burst where every shard
// flushes its segment around the same wallclock moment. Without it,
// every shard's flush would trigger a separate root MPD rewrite.
func (m *ABRMaster) UpdateShard(snap ShardSnapshot) {
	if snap.Slug == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.stopped {
		return
	}
	m.shards[snap.Slug] = snap
	if m.debounce != nil {
		m.debounce.Stop()
	}
	m.debounce = time.AfterFunc(150*time.Millisecond, m.flushRoot)
}

// Stop cancels the pending debounced flush and triggers one final root
// MPD write so the on-disk MPD reflects the latest snapshots when
// callers tear down the stream.
func (m *ABRMaster) Stop() {
	m.mu.Lock()
	if m.debounce != nil {
		m.debounce.Stop()
	}
	m.stopped = true
	m.mu.Unlock()
	m.flushRoot()
}

// flushRoot builds the root MPD from the latest shard snapshots and
// writes it atomically. Called by the debounce timer and by Stop.
//
// Locking: copy out the shards under m.mu, then release the lock for
// the I/O. This guarantees flushRoot never holds m.mu while doing disk
// work — important because UpdateShard runs under per-shard mutexes
// and the original v1 implementation had a deadlock here.
func (m *ABRMaster) flushRoot() {
	m.mu.Lock()
	if len(m.shards) == 0 {
		m.mu.Unlock()
		return
	}
	snaps := make([]ShardSnapshot, 0, len(m.shards))
	for _, s := range m.shards {
		snaps = append(snaps, s)
	}
	rootManifest := m.rootManifest
	segDur := m.segDur
	window := m.window
	streamID := m.streamID
	m.mu.Unlock()

	// Stable rendition order: sort by slug. The order determines the
	// listing in the master MPD, which players use to pick the default
	// rendition.
	sort.Slice(snaps, func(i, j int) bool { return snaps[i].Slug < snaps[j].Slug })

	in := combineSnapshots(snaps, segDur, window)
	if in == nil {
		return
	}
	in.PublishTime = time.Now()

	data, err := BuildManifest(in)
	if err != nil {
		slog.Warn("dash: abr master build manifest", "stream_id", streamID, "err", err)
		return
	}
	if data == nil {
		return
	}
	if err := writeFileAtomic(rootManifest, data); err != nil {
		slog.Warn("dash: abr master write manifest", "stream_id", streamID, "err", err)
	}
}

// combineSnapshots collapses N shard snapshots into a single ManifestInput
// where Video is the concatenated list of video representations and
// Audio is the FIRST snapshot's audio (audio is shared across the
// ladder — the primary shard packs it, the rest pass nil).
//
// Returns nil when no shard has either track populated yet.
func combineSnapshots(snaps []ShardSnapshot, segDur time.Duration, window int) *ManifestInput {
	in := &ManifestInput{
		SegDur: segDur,
		Window: window,
	}
	for _, s := range snaps {
		if s.Video != nil && len(s.Video.Segments) > 0 {
			in.Video = append(in.Video, *s.Video)
		}
		// AST is set from whichever shard provides a non-zero value
		// first. All shards should converge on the same AST since
		// pairing is per-shard but AST is per-stream — log if they
		// diverge to surface coordinator bugs.
		if in.AvailabilityStart.IsZero() && !s.AvailabilityStart.IsZero() {
			in.AvailabilityStart = s.AvailabilityStart
		}
		// Audio: take the FIRST shard whose Audio is non-nil + has
		// segments. Subsequent shards' Audio is ignored (the primary
		// shard packs audio; others don't).
		if in.Audio == nil && s.Audio != nil && len(s.Audio.Segments) > 0 {
			in.Audio = s.Audio
		}
	}
	if len(in.Video) == 0 && in.Audio == nil {
		return nil
	}
	return in
}

// writeFileAtomic writes data to path via a temp file + rename, so
// readers never see a half-written file. The temp file lives in the
// same directory so rename(2) is atomic on most filesystems.
func writeFileAtomic(path string, data []byte) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	f, err := os.CreateTemp(dir, ".tmp-")
	if err != nil {
		return err
	}
	tmpName := f.Name()
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	return os.Rename(tmpName, path)
}
