package blob

import (
	"bytes"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/Eyevinn/mp4ff/aac"

	"github.com/datvietvac-techhub/open-streamer/internal/buffer"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/publisher/dash"
)

const (
	videoTimescale = 90000
	// DefaultSegDur is the target fragment duration when the recording config
	// doesn't specify one.
	DefaultSegDur = 6 * time.Second
	maxSegFactor  = 4 // safety-net cut at maxSegFactor × segDur with no IDR
)

// hourRecordSink lets the owning Service fold a writer's sealed/updated hour
// summary into the per-stream catalog. Called synchronously from the lane; nil
// is allowed (tests inspect the blob/ranges directly).
type hourRecordSink func(profileID string, rec HourRecord)

// profileWriter records ONE profile lane into per-hour CMAF blobs. It is NOT
// goroutine-safe — the Service drives it from a single lane goroutine. It reuses
// the live packager's FrameQueue + Segmenter + fMP4 builders so an archived
// fragment is byte-identical to a live one; the only added timeline math is the
// PTS-derived, restart-resumable tfdt (§3.1.3 of the design).
type profileWriter struct {
	streamDir string
	profileID string
	isAudio   bool // this lane writes .cmfa
	segDur    time.Duration
	sink      hourRecordSink

	q   *dash.FrameQueue
	seg *dash.Segmenter

	videoInit *dash.VideoInit
	audioInit *dash.AudioInit
	firstIDR  bool // a video IDR has been seen (startup gate)

	// PTS-derived absolute timeline anchor.
	originSet    bool
	originPTSms  uint64
	originWallMs int64

	// open hour (zero w.hour = none open).
	hour        time.Time
	stem        string
	vBlob       *blobFile
	aBlob       *blobFile
	rf          *rangesFile
	vSeq        uint32
	aSeq        uint32
	pendingDisc bool

	// monotonic tfdt guards.
	lastVTfdt uint64
	haveVTfdt bool
	lastATfdt uint64
	haveATfdt bool

	// in-progress hour summary (flushed to the sink on seal / close).
	cur HourRecord
}

// newProfileWriter creates a lane writer for profileID under streamDir.
func newProfileWriter(streamDir, profileID string, isAudio bool, segDur time.Duration, sink hourRecordSink) *profileWriter {
	if segDur <= 0 {
		segDur = DefaultSegDur
	}
	return &profileWriter{
		streamDir: streamDir, profileID: profileID, isAudio: isAudio,
		segDur: segDur, sink: sink,
		q:   dash.NewFrameQueue(),
		seg: dash.NewSegmenter(segDur, maxSegFactor),
	}
}

// Origin reports the persisted wall<->media anchor (media ticks are relative to
// the recording origin, so OriginTicks is always 0). set is false until the
// first video frame has been ingested.
func (w *profileWriter) Origin() (ticks uint64, wallMs int64, set bool) {
	return 0, w.originWallMs, w.originSet
}

// Tick drains a safety-net cut whose wallclock deadline has passed without new
// packets (a stalled source). The lane goroutine calls it periodically so a
// stream that pauses still flushes its buffered fragment.
func (w *profileWriter) Tick(now time.Time) error { return w.drainCuts(now) }

// Ingest pushes one buffer packet's frames into the queue and drains any
// fragments the segmenter is ready to cut. now is the receive wallclock, used
// to anchor the recording origin and drive the safety-net cut deadline.
func (w *profileWriter) Ingest(pkt buffer.Packet, now time.Time) error {
	if pkt.SessionStart {
		// Session boundary (failover/reconnect): the live packager drops; the
		// DVR resets pairing state and flags the next fragment discontinuous.
		// (Flush-then-mark is a later refinement; v1 accepts a sub-segment loss
		// at the seam, masked by EXT-X-DISCONTINUITY on read.)
		w.q = dash.NewFrameQueue()
		w.seg.Reset()
		w.pendingDisc = true
	}
	if pkt.AV != nil {
		if err := w.ingestAV(pkt.AV, now); err != nil {
			return err
		}
	}
	// pkt.TS (copy:// passthrough) ingress lands with the TS-demuxer wiring in a
	// follow-up; transcoded streams use the AV path above.
	return w.drainCuts(now)
}

func (w *profileWriter) ingestAV(av *domain.AVPacket, now time.Time) error {
	switch {
	case av.Codec.IsVideo():
		if !w.originSet {
			w.originSet = true
			w.originPTSms = av.PTSms
			w.originWallMs = now.UTC().UnixMilli()
		}
		w.ensureVideoInit(av)
		if w.videoInit == nil {
			return nil // still waiting for a usable IDR + parameter sets
		}
		if !w.firstIDR {
			if !av.KeyFrame {
				return nil // drop leading non-IDR so every segment opens at a SAP
			}
			w.firstIDR = true
		}
		w.q.PushVideo(dash.VideoFrame{
			AnnexB: append([]byte(nil), av.Data...),
			PTSms:  av.PTSms, DTSms: av.DTSms, IsIDR: av.KeyFrame,
		})
	case w.isAudio && av.Codec == domain.AVCodecAAC:
		w.ensureAudioInit(av)
		if w.audioInit == nil {
			return nil
		}
		for _, f := range dash.SplitADTSBundle(av.Data, av.PTSms) {
			w.q.PushAudio(f)
		}
	}
	return nil
}

// ensureVideoInit lazily builds the video init from the first IDR's parameter
// sets. IDR access units reaching the buffer hub carry an in-band SPS/PPS prefix
// (an RTMP/RTSP invariant; the transcoder emits them with every IDR).
func (w *profileWriter) ensureVideoInit(av *domain.AVPacket) {
	if w.videoInit != nil || !av.KeyFrame {
		return
	}
	var (
		vi  *dash.VideoInit
		err error
	)
	if av.Codec == domain.AVCodecH265 {
		vpss, spss, ppss := dash.ExtractHEVCParameterSets(av.Data)
		vi, err = dash.BuildH265Init(vpss, spss, ppss)
	} else {
		spss, ppss := dash.ExtractParameterSets(av.Data)
		vi, err = dash.BuildH264Init(spss, ppss)
	}
	if err == nil {
		w.videoInit = vi
	}
}

// ensureAudioInit lazily builds the AAC init from the first ADTS header.
func (w *profileWriter) ensureAudioInit(av *domain.AVPacket) {
	if w.audioInit != nil {
		return
	}
	hdr, _, err := aac.DecodeADTSHeader(bytes.NewReader(av.Data))
	if err != nil || hdr == nil {
		return
	}
	if ai, err := dash.BuildAACInit(hdr); err == nil {
		w.audioInit = ai
	}
}

// drainCuts emits every fragment the segmenter is ready to cut.
func (w *profileWriter) drainCuts(now time.Time) error {
	for {
		haveVideo := w.videoInit != nil
		haveAudio := w.isAudio && w.audioInit != nil
		var audioSR uint64
		if haveAudio {
			audioSR = uint64(w.audioInit.SampleRate) //nolint:gosec // sample rate is a small positive int
		}
		dec := w.seg.Cut(now, w.q, haveVideo, haveAudio, audioSR)
		if !dec.Ok {
			return nil
		}
		if err := w.emit(dec, now); err != nil {
			return err
		}
	}
}

// emit pops the decided frames, builds the fragment(s), and appends them to the
// current hour blob + range index, rotating the hour first if the fragment's
// wallclock crosses an hour boundary.
func (w *profileWriter) emit(dec dash.CutDecision, now time.Time) error {
	if dec.VideoCount == 0 {
		// Audio-only cut shouldn't occur on a V+A lane; drop defensively.
		_ = w.q.PopAudio(dec.AudioCount)
		return nil
	}
	vframes := w.q.PopVideo(dec.VideoCount)
	if len(vframes) == 0 {
		return nil
	}
	nextPTS, hasNext := uint64(0), false
	if nf, ok := w.q.FirstVideo(); ok {
		nextPTS, hasNext = nf.PTSms, true
	}
	segDurTicks := dash.ComputeVideoSegDurTicks(vframes, nextPTS, hasNext)

	firstPTS := vframes[0].PTSms
	wallMs := w.originWallMs + w.deltaMs(firstPTS)
	if err := w.ensureHour(wallMs); err != nil {
		return err
	}

	tfdtV := w.tfdtVideo(firstPTS)
	disc := w.pendingDisc
	w.pendingDisc = false

	frag, err := dash.BuildVideoFragment(w.nextVSeq(), w.videoInit.TrackID, tfdtV, vframes, w.videoInit.Info.IsHEVC, segDurTicks)
	if err != nil {
		return fmt.Errorf("blob: build video fragment: %w", err)
	}
	off, err := w.vBlob.Append(frag)
	if err != nil {
		return err
	}
	if err := w.rf.Append(FragRecord{
		Track: TrackVideo, Keyframe: vframes[0].IsIDR, Discontinuity: disc,
		SampleCount: uint32(len(vframes)), //nolint:gosec // GOP size is small
		WallTimeMs:  wallMs, MediaTicks: tfdtV,
		ByteOffset: uint64(off), ByteLen: uint32(len(frag)), DurTicks: uint32(segDurTicks), //nolint:gosec // bounded
	}); err != nil {
		return err
	}
	w.noteVideoFrag(wallMs, tfdtV)

	if w.isAudio && dec.AudioCount > 0 {
		if err := w.emitAudio(dec.AudioCount); err != nil {
			return err
		}
	}
	w.seg.MarkCut(now)
	w.checkpointHour()
	return nil
}

// checkpointHour flushes the in-progress hour's running summary to the catalog
// after each fragment, so recording_status reflects coverage/size near-realtime
// (matching the legacy per-segment index updates) instead of only on hour seal.
func (w *profileWriter) checkpointHour() {
	if w.sink == nil || w.hour.IsZero() {
		return
	}
	w.cur.SizeBytes = w.hourSize()
	w.sink(w.profileID, w.cur) // cur.Sealed is false until sealHour
}

// hourFilesExist reports whether any blob file for this hour stem is present.
func hourFilesExist(stem string) bool {
	for _, ext := range []string{".cmfv", ".cmfa", ".ranges", ".open"} {
		if _, err := os.Stat(stem + ext); err == nil {
			return true
		}
	}
	return false
}

// removeHourFiles deletes all blob files for an hour stem (best effort).
func removeHourFiles(stem string) {
	for _, ext := range []string{".cmfv", ".cmfa", ".ranges", ".ranges.tmp", ".open"} {
		_ = os.Remove(stem + ext)
	}
}

func (w *profileWriter) emitAudio(count int) error {
	aframes := w.q.PopAudio(count)
	if len(aframes) == 0 {
		return nil
	}
	if err := w.ensureAudioBlob(); err != nil {
		return err
	}
	sr := uint64(w.audioInit.SampleRate) //nolint:gosec // positive
	tfdtA := w.tfdtAudio(aframes[0].PTSms, sr)
	frag, err := dash.BuildAudioFragment(w.nextASeq(), w.audioInit.TrackID, tfdtA, aframes)
	if err != nil {
		return fmt.Errorf("blob: build audio fragment: %w", err)
	}
	off, err := w.aBlob.Append(frag)
	if err != nil {
		return err
	}
	durTicks := uint64(len(aframes)) * 1024
	if err := w.rf.Append(FragRecord{
		Track: TrackAudio, SampleCount: uint32(len(aframes)), //nolint:gosec // small
		WallTimeMs: w.originWallMs + w.deltaMs(aframes[0].PTSms), MediaTicks: tfdtA,
		ByteOffset: uint64(off), ByteLen: uint32(len(frag)), DurTicks: uint32(durTicks), //nolint:gosec // bounded
	}); err != nil {
		return err
	}
	w.cur.FragCountA++
	return nil
}

// deltaMs returns ptsMs - originPTSms as a signed delta (handles the rare
// pre-origin frame defensively).
func (w *profileWriter) deltaMs(ptsMs uint64) int64 {
	return int64(ptsMs) - int64(w.originPTSms) //nolint:gosec // ms values fit int64
}

func (w *profileWriter) tfdtVideo(ptsMs uint64) uint64 {
	t := w.mediaTicks(ptsMs, videoTimescale)
	if w.haveVTfdt && !w.pendingDisc && t <= w.lastVTfdt {
		t = w.lastVTfdt + 1
	}
	w.lastVTfdt, w.haveVTfdt = t, true
	return t
}

func (w *profileWriter) tfdtAudio(ptsMs, sr uint64) uint64 {
	t := w.mediaTicks(ptsMs, sr)
	if w.haveATfdt && !w.pendingDisc && t <= w.lastATfdt {
		t = w.lastATfdt + 1
	}
	w.lastATfdt, w.haveATfdt = t, true
	return t
}

// mediaTicks maps a PTS (ms) to absolute ticks at timescale, relative to the
// recording origin.
func (w *profileWriter) mediaTicks(ptsMs, timescale uint64) uint64 {
	d := w.deltaMs(ptsMs)
	if d < 0 {
		d = 0
	}
	return uint64(d) * timescale / 1000
}

func (w *profileWriter) nextVSeq() uint32 { w.vSeq++; return w.vSeq }
func (w *profileWriter) nextASeq() uint32 { w.aSeq++; return w.aSeq }

func (w *profileWriter) noteVideoFrag(wallMs int64, tfdtV uint64) {
	if w.cur.FragCountV == 0 {
		w.cur.WallFromMs = wallMs
		w.cur.MediaFromV = tfdtV
	}
	w.cur.WallToMs = wallMs
	w.cur.MediaToV = tfdtV
	w.cur.FragCountV++
}

// ensureHour opens the hour blob for wallMs, rotating (sealing the previous
// hour) when wallMs crosses into a new UTC hour.
func (w *profileWriter) ensureHour(wallMs int64) error {
	target := hourFloor(time.UnixMilli(wallMs))
	if !w.hour.IsZero() && target.Equal(w.hour) {
		return nil
	}
	if !w.hour.IsZero() {
		if err := w.sealHour(); err != nil {
			return err
		}
	}
	return w.openHour(target)
}

func (w *profileWriter) openHour(h time.Time) error {
	pdir := profileDirPath(w.streamDir, w.profileID)
	hdir := hourDirPath(pdir, h)
	if err := ensureDir(hdir); err != nil {
		return err
	}
	stem := hourStem(hdir, hourToken(h))
	// Resume into an existing wall-hour (e.g. a mid-hour restart): the blob files
	// for this hour are already on disk and createBlobFile is O_EXCL, which would
	// fail and kill the lane. Discard the stale partial hour and start it fresh —
	// recording continuity wins over the partial hour (not recovered, by design).
	if hourFilesExist(stem) {
		slog.Warn("blob: resuming into existing hour; discarding partial hour", "profile", w.profileID, "hour", hourRel(h))
		removeHourFiles(stem)
	}
	vinit, err := dash.EncodeInit(w.videoInit.Init)
	if err != nil {
		return fmt.Errorf("blob: encode video init: %w", err)
	}
	vb, err := createBlobFile(stem+".cmfv", vinit)
	if err != nil {
		return err
	}
	rf, err := createRangesFile(stem+".ranges", RangesHeader{
		VideoInitLen:    uint32(len(vinit)), //nolint:gosec // init is a few KB
		VideoTimescale:  videoTimescale,
		HourStartUnixMs: h.UnixMilli(),
	})
	if err != nil {
		_ = vb.Close()
		return err
	}
	if err := createSentinel(stem); err != nil {
		_ = vb.Close()
		_ = rf.Close()
		return err
	}
	_ = fsyncDir(hdir)
	w.hour, w.stem, w.vBlob, w.rf = h, stem, vb, rf
	w.aBlob = nil
	w.vSeq, w.aSeq = 0, 0
	w.cur = HourRecord{Hour: hourRel(h)}
	return nil
}

// ensureAudioBlob lazily creates the hour's .cmfa on the first audio fragment
// (the audio init is known by then) and records its length in the ranges header.
func (w *profileWriter) ensureAudioBlob() error {
	if w.aBlob != nil {
		return nil
	}
	ainit, err := dash.EncodeInit(w.audioInit.Init)
	if err != nil {
		return fmt.Errorf("blob: encode audio init: %w", err)
	}
	ab, err := createBlobFile(w.stem+".cmfa", ainit)
	if err != nil {
		return err
	}
	w.aBlob = ab
	hdr := w.rf.Header()
	hdr.AudioInitLen = uint32(len(ainit))               //nolint:gosec // init is a few KB
	hdr.AudioTimescale = uint32(w.audioInit.SampleRate) //nolint:gosec // positive
	w.rf.hdr = hdr
	return w.rf.WriteHeader()
}

// sealHour fsyncs + seals the current hour and reports it to the catalog sink.
func (w *profileWriter) sealHour() error {
	if w.vBlob != nil {
		if err := w.vBlob.Sync(); err != nil {
			return err
		}
	}
	if w.aBlob != nil {
		if err := w.aBlob.Sync(); err != nil {
			return err
		}
	}
	if w.rf != nil {
		if err := w.rf.Seal(); err != nil {
			return err
		}
	}
	if err := removeSentinel(w.stem); err != nil {
		return err
	}
	w.cur.Sealed = true
	w.cur.SizeBytes = w.hourSize()
	if w.sink != nil {
		w.sink(w.profileID, w.cur)
	}
	w.closeHourFDs()
	return nil
}

func (w *profileWriter) hourSize() int64 {
	var n int64
	if w.vBlob != nil {
		n += w.vBlob.Size()
	}
	if w.aBlob != nil {
		n += w.aBlob.Size()
	}
	return n
}

func (w *profileWriter) closeHourFDs() {
	if w.vBlob != nil {
		_ = w.vBlob.Close()
	}
	if w.aBlob != nil {
		_ = w.aBlob.Close()
	}
	if w.rf != nil {
		_ = w.rf.Close()
	}
	w.vBlob, w.aBlob, w.rf = nil, nil, nil
}

// Close seals the in-progress hour and releases all FDs.
func (w *profileWriter) Close() error {
	if w.hour.IsZero() {
		return nil
	}
	err := w.sealHour()
	w.hour = time.Time{}
	return err
}
