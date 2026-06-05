package blob

import (
	"fmt"
	"os"
)

// blobFile is one hour container (HH.cmfv or HH.cmfa): an init at offset 0
// followed by concatenated [styp][moof][mdat] fragments. It tracks the running
// write offset so Append returns each fragment's start offset for the index.
type blobFile struct {
	f    *os.File
	path string
	size int64
}

// createBlobFile creates a fresh hour blob and writes the init segment at its
// head. It fails if the file already exists (a fresh hour must be new).
func createBlobFile(path string, init []byte) (*blobFile, error) {
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("blob: create %s: %w", path, err)
	}
	b := &blobFile{f: f, path: path}
	if len(init) > 0 {
		if _, err := f.Write(init); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("blob: write init %s: %w", path, err)
		}
		b.size = int64(len(init))
	}
	return b, nil
}

// openBlobFile opens an existing hour blob for append + read (resume path),
// seeking the running offset to the current end of file.
func openBlobFile(path string) (*blobFile, error) {
	f, err := os.OpenFile(path, os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("blob: open %s: %w", path, err)
	}
	fi, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("blob: stat %s: %w", path, err)
	}
	return &blobFile{f: f, path: path, size: fi.Size()}, nil
}

// Append writes one fragment at the current end and returns its start offset.
func (b *blobFile) Append(p []byte) (off int64, err error) {
	off = b.size
	n, err := b.f.WriteAt(p, off)
	if err != nil {
		return 0, fmt.Errorf("blob: append %s: %w", b.path, err)
	}
	b.size += int64(n)
	return off, nil
}

// ReadSlice returns length bytes starting at off — the exact stored fragment
// (or init) bytes, served verbatim with no re-mux.
func (b *blobFile) ReadSlice(off int64, length int) ([]byte, error) {
	buf := make([]byte, length)
	if _, err := b.f.ReadAt(buf, off); err != nil {
		return nil, fmt.Errorf("blob: read %s @%d+%d: %w", b.path, off, length, err)
	}
	return buf, nil
}

// Truncate cuts the blob back to size (discarding a torn tail on recovery).
func (b *blobFile) Truncate(size int64) error {
	if err := b.f.Truncate(size); err != nil {
		return fmt.Errorf("blob: truncate %s: %w", b.path, err)
	}
	b.size = size
	return nil
}

// Sync flushes the blob to stable storage.
func (b *blobFile) Sync() error { return b.f.Sync() }

// Size is the current end-of-file (running append offset).
func (b *blobFile) Size() int64 { return b.size }

// Close releases the underlying file descriptor.
func (b *blobFile) Close() error { return b.f.Close() }

// rangesFile is the per-hour ".ranges" index: a 64-byte header then fixed-width
// fragment records appended in blob order. The header (RecordCount, Flags) is
// rewritten in place at each checkpoint.
type rangesFile struct {
	f     *os.File
	path  string
	hdr   RangesHeader
	count uint32
}

// createRangesFile creates a fresh index and writes its header (count = 0).
func createRangesFile(path string, hdr RangesHeader) (*rangesFile, error) {
	hdr.Version = rangesVersion
	hdr.RecordSize = FragRecordSize
	hdr.RecordCount = 0
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return nil, fmt.Errorf("blob: create ranges %s: %w", path, err)
	}
	r := &rangesFile{f: f, path: path, hdr: hdr}
	head := MarshalRangesHeader(hdr)
	if _, err := f.WriteAt(head[:], 0); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("blob: write ranges header %s: %w", path, err)
	}
	return r, nil
}

// Append writes one record after the last and bumps the in-memory count. The
// durable RecordCount is only advanced by WriteHeader at a checkpoint.
func (r *rangesFile) Append(rec FragRecord) error {
	b := MarshalFragRecord(rec)
	off := int64(RangesHeadSize) + int64(r.count)*FragRecordSize
	if _, err := r.f.WriteAt(b[:], off); err != nil {
		return fmt.Errorf("blob: append ranges %s: %w", r.path, err)
	}
	r.count++
	return nil
}

// WriteHeader rewrites the 64-byte header in place with the current count and
// flags — the commit point that makes appended records durable-countable.
func (r *rangesFile) WriteHeader() error {
	r.hdr.RecordCount = r.count
	head := MarshalRangesHeader(r.hdr)
	if _, err := r.f.WriteAt(head[:], 0); err != nil {
		return fmt.Errorf("blob: rewrite ranges header %s: %w", r.path, err)
	}
	return nil
}

// Seal marks the hour closed cleanly and persists the header.
func (r *rangesFile) Seal() error {
	r.hdr.Flags |= HeaderFlagSealed
	if err := r.WriteHeader(); err != nil {
		return err
	}
	return r.Sync()
}

// Truncate cuts the index back to count records (recovery), persisting the
// repaired header.
func (r *rangesFile) Truncate(count uint32) error {
	if err := r.f.Truncate(int64(RangesHeadSize) + int64(count)*FragRecordSize); err != nil {
		return fmt.Errorf("blob: truncate ranges %s: %w", r.path, err)
	}
	r.count = count
	return r.WriteHeader()
}

// Sync flushes the index to stable storage.
func (r *rangesFile) Sync() error { return r.f.Sync() }

// Count is the number of records appended (durable after WriteHeader).
func (r *rangesFile) Count() uint32 { return r.count }

// Header returns the in-memory header.
func (r *rangesFile) Header() RangesHeader { return r.hdr }

// Close releases the underlying file descriptor.
func (r *rangesFile) Close() error { return r.f.Close() }

// ReadRanges loads a ".ranges" file and returns the header plus every record
// that passes CRC + a bounds check against blobSize (so a torn or
// not-yet-durable tail record is dropped). It stops at the first bad record —
// the index is append-only, so a bad record means the end of valid data.
// blobSize is the size of the corresponding container; pass a large value to
// skip the bounds check (header-only validation).
func ReadRanges(path string, blobSize int64) (RangesHeader, []FragRecord, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return RangesHeader{}, nil, fmt.Errorf("blob: read ranges %s: %w", path, err)
	}
	hdr, err := UnmarshalRangesHeader(data)
	if err != nil {
		return RangesHeader{}, nil, err
	}
	recs := make([]FragRecord, 0, hdr.RecordCount)
	for off := RangesHeadSize; off+FragRecordSize <= len(data); off += FragRecordSize {
		rec, ok := UnmarshalFragRecord(data[off : off+FragRecordSize])
		if !ok {
			break // torn / corrupt tail
		}
		if int64(rec.ByteOffset)+int64(rec.ByteLen) > blobSize {
			break // fragment bytes not durable yet
		}
		recs = append(recs, rec)
	}
	return hdr, recs, nil
}

// NOTE: the ".open" sentinel, directory-fsync, mkdir-p and hour-stem helpers
// land with the writer lane (Phase 1b), which is their only caller.
