// Package blob implements the CMAF blob-archive DVR: per-hour container files
// of concatenated fMP4 fragments, a per-hour binary range index, and a
// per-stream catalog. See docs/todo/dvr-blob-archive.md for the full design.
//
// This file defines the on-disk ".ranges" index format: a 64-byte header
// followed by fixed-width 40-byte fragment records. The format is append-only
// and little-endian so a crash truncates cleanly at a record boundary and a
// reader can binary-search by media time.
package blob

import (
	"encoding/binary"
	"fmt"
)

// On-disk constants for the ".ranges" index. RecordSize is also stored in the
// header so a future wider record stays forward-readable.
const (
	rangesMagic    = "OSR1"
	rangesVersion  = 1
	RangesHeadSize = 64
	FragRecordSize = 40
)

// Track identifies which elementary stream a fragment record belongs to.
const (
	TrackVideo uint8 = 0
	TrackAudio uint8 = 1
)

// Header flag bits (RangesHeader.Flags).
const (
	HeaderFlagSealed       uint16 = 1 << 0 // hour closed cleanly
	HeaderFlagStartDiscont uint16 = 1 << 1 // hour's first fragment carries a discontinuity
)

// Record flag bits (FragRecord.Flags).
const (
	RecFlagKeyframe      uint8 = 1 << 0 // fragment starts with an IDR/IRAP
	RecFlagDiscontinuity uint8 = 1 << 1 // discontinuity-before this fragment
)

// RangesHeader is the 64-byte fixed header at offset 0 of every ".ranges"
// file. It is rewritten in place on each checkpoint (RecordCount, Flags).
type RangesHeader struct {
	Version         uint16
	RecordSize      uint16
	Flags           uint16
	VideoInitLen    uint32 // length of the video init at offset 0 of HH.cmfv (0 if no video)
	AudioInitLen    uint32 // length of the audio init at offset 0 of HH.cmfa (0 if no .cmfa)
	VideoTimescale  uint32
	AudioTimescale  uint32
	RecordCount     uint32 // commit hint — recovery reconciles against the blob
	HourStartUnixMs int64  // UTC ms at the top of this hour (the YYYYMMDDHH anchor)
	BaseMediaTicksV uint64 // absolute video tfdt of this hour's first video fragment
	BaseMediaTicksA uint64 // absolute audio tfdt of this hour's first audio fragment
}

// MarshalRangesHeader encodes h into a 64-byte little-endian buffer.
func MarshalRangesHeader(h RangesHeader) [RangesHeadSize]byte {
	var b [RangesHeadSize]byte
	copy(b[0:4], rangesMagic)
	binary.LittleEndian.PutUint16(b[4:6], h.Version)
	binary.LittleEndian.PutUint16(b[6:8], h.RecordSize)
	binary.LittleEndian.PutUint16(b[8:10], h.Flags)
	// b[10:12] pad
	binary.LittleEndian.PutUint32(b[12:16], h.VideoInitLen)
	binary.LittleEndian.PutUint32(b[16:20], h.AudioInitLen)
	binary.LittleEndian.PutUint32(b[20:24], h.VideoTimescale)
	binary.LittleEndian.PutUint32(b[24:28], h.AudioTimescale)
	binary.LittleEndian.PutUint32(b[28:32], h.RecordCount)
	binary.LittleEndian.PutUint64(b[32:40], uint64(h.HourStartUnixMs)) //nolint:gosec // round-trips bit-for-bit
	binary.LittleEndian.PutUint64(b[40:48], h.BaseMediaTicksV)
	binary.LittleEndian.PutUint64(b[48:56], h.BaseMediaTicksA)
	// b[56:64] reserved
	return b
}

// UnmarshalRangesHeader decodes a 64-byte header, validating magic/version.
func UnmarshalRangesHeader(b []byte) (RangesHeader, error) {
	if len(b) < RangesHeadSize {
		return RangesHeader{}, fmt.Errorf("blob: ranges header: short buffer (%d < %d)", len(b), RangesHeadSize)
	}
	if string(b[0:4]) != rangesMagic {
		return RangesHeader{}, fmt.Errorf("blob: ranges header: bad magic %q", b[0:4])
	}
	h := RangesHeader{
		Version:         binary.LittleEndian.Uint16(b[4:6]),
		RecordSize:      binary.LittleEndian.Uint16(b[6:8]),
		Flags:           binary.LittleEndian.Uint16(b[8:10]),
		VideoInitLen:    binary.LittleEndian.Uint32(b[12:16]),
		AudioInitLen:    binary.LittleEndian.Uint32(b[16:20]),
		VideoTimescale:  binary.LittleEndian.Uint32(b[20:24]),
		AudioTimescale:  binary.LittleEndian.Uint32(b[24:28]),
		RecordCount:     binary.LittleEndian.Uint32(b[28:32]),
		HourStartUnixMs: int64(binary.LittleEndian.Uint64(b[32:40])), //nolint:gosec // round-trips bit-for-bit
		BaseMediaTicksV: binary.LittleEndian.Uint64(b[40:48]),
		BaseMediaTicksA: binary.LittleEndian.Uint64(b[48:56]),
	}
	if h.Version != rangesVersion {
		return RangesHeader{}, fmt.Errorf("blob: ranges header: unsupported version %d", h.Version)
	}
	if h.RecordSize < FragRecordSize {
		return RangesHeader{}, fmt.Errorf("blob: ranges header: record size %d too small", h.RecordSize)
	}
	return h, nil
}

// FragRecord is one 40-byte fragment index record. ByteOffset points at the
// fragment's styp box; ByteLen covers the whole [styp][moof][mdat] run.
type FragRecord struct {
	Track         uint8
	Keyframe      bool
	Discontinuity bool
	SampleCount   uint32
	WallTimeMs    int64  // derived UTC ms of the first sample (search + PROGRAM-DATE-TIME)
	MediaTicks    uint64 // absolute tfdt of the first sample — PTS-derived, continuous across hours
	ByteOffset    uint64 // offset of the styp box in the blob
	ByteLen       uint32 // [styp][moof][mdat] length
	DurTicks      uint32 // fragment duration in the track timescale
}

func (r FragRecord) flags() uint8 {
	var f uint8
	if r.Keyframe {
		f |= RecFlagKeyframe
	}
	if r.Discontinuity {
		f |= RecFlagDiscontinuity
	}
	return f
}

// MarshalFragRecord encodes r into a 40-byte buffer, computing the CRC16 over
// every field except the CRC slot itself (bytes [0,2)+[4,40)), so a torn final
// record written by a crash mid-append is detectable on recovery.
func MarshalFragRecord(r FragRecord) [FragRecordSize]byte {
	var b [FragRecordSize]byte
	b[0] = r.Track
	b[1] = r.flags()
	// b[2:4] CRC16, filled last
	binary.LittleEndian.PutUint32(b[4:8], r.SampleCount)
	binary.LittleEndian.PutUint64(b[8:16], uint64(r.WallTimeMs)) //nolint:gosec // round-trips bit-for-bit
	binary.LittleEndian.PutUint64(b[16:24], r.MediaTicks)
	binary.LittleEndian.PutUint64(b[24:32], r.ByteOffset)
	binary.LittleEndian.PutUint32(b[32:36], r.ByteLen)
	binary.LittleEndian.PutUint32(b[36:40], r.DurTicks)
	binary.LittleEndian.PutUint16(b[2:4], fragRecordCRC(b))
	return b
}

// UnmarshalFragRecord decodes a 40-byte record and verifies its CRC16.
// ok is false when the stored CRC doesn't match (a torn / corrupt record).
func UnmarshalFragRecord(b []byte) (rec FragRecord, ok bool) {
	if len(b) < FragRecordSize {
		return FragRecord{}, false
	}
	var fixed [FragRecordSize]byte
	copy(fixed[:], b[:FragRecordSize])
	want := binary.LittleEndian.Uint16(fixed[2:4])
	if want != fragRecordCRC(fixed) {
		return FragRecord{}, false
	}
	flags := fixed[1]
	return FragRecord{
		Track:         fixed[0],
		Keyframe:      flags&RecFlagKeyframe != 0,
		Discontinuity: flags&RecFlagDiscontinuity != 0,
		SampleCount:   binary.LittleEndian.Uint32(fixed[4:8]),
		WallTimeMs:    int64(binary.LittleEndian.Uint64(fixed[8:16])), //nolint:gosec // round-trips bit-for-bit
		MediaTicks:    binary.LittleEndian.Uint64(fixed[16:24]),
		ByteOffset:    binary.LittleEndian.Uint64(fixed[24:32]),
		ByteLen:       binary.LittleEndian.Uint32(fixed[32:36]),
		DurTicks:      binary.LittleEndian.Uint32(fixed[36:40]),
	}, true
}

// fragRecordCRC computes the CRC-16/CCITT-FALSE over the record bytes excluding
// the 2-byte CRC slot at [2,4): i.e. [0,2) ++ [4,40).
func fragRecordCRC(b [FragRecordSize]byte) uint16 {
	crc := crc16Update(0xFFFF, b[0:2])
	crc = crc16Update(crc, b[4:FragRecordSize])
	return crc
}

// crc16Update folds data into a running CRC-16/CCITT-FALSE (poly 0x1021).
func crc16Update(crc uint16, data []byte) uint16 {
	for _, c := range data {
		crc ^= uint16(c) << 8
		for i := 0; i < 8; i++ {
			if crc&0x8000 != 0 {
				crc = (crc << 1) ^ 0x1021
			} else {
				crc <<= 1
			}
		}
	}
	return crc
}

// SearchByMedia returns the index of the last record (within recs, assumed
// sorted ascending by MediaTicks for the given track) whose MediaTicks <= ticks,
// or -1 if every record starts after ticks. recs must already be filtered to a
// single track. Callers do keyframe-snapping on top of this.
func SearchByMedia(recs []FragRecord, ticks uint64) int {
	lo, hi, ans := 0, len(recs)-1, -1
	for lo <= hi {
		mid := int(uint(lo+hi) >> 1)
		if recs[mid].MediaTicks <= ticks {
			ans = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}
	return ans
}
