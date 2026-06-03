// Package json provides a single-file JSON implementation of the store repositories.
// All data (streams, recordings, hooks, global config) is stored in one file
// with top-level keys, e.g.:
//
//	{
//	  "streams":    { "<code>": {...}, ... },
//	  "recordings": { "<id>":   {...}, ... },
//	  "hooks":      { "<id>":   {...}, ... },
//	  "global":     {}
//	}
//
// Writes are atomic: data is marshalled to a .tmp file then renamed into place.
// Intended for development and lightweight single-node deployments.
package json

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
)

// dbFile is the name of the single data file inside the configured directory.
const dbFile = "open_streamer.json"

// db is the top-level structure persisted to disk.
// Additional top-level keys (e.g. "global") can be added here as the product grows.
type db struct {
	Streams    map[string]*domain.Stream    `json:"streams"`
	Templates  map[string]*domain.Template  `json:"templates,omitempty"`
	Recordings map[string]*domain.Recording `json:"recordings"`
	Hooks      map[string]*domain.Hook      `json:"hooks"`
	VOD        map[string]*domain.VODMount  `json:"vod,omitempty"`
	Global     *domain.GlobalConfig         `json:"global,omitempty"`
}

// Store is a JSON-backed implementation of all repositories.
type Store struct {
	file string
	mu   sync.RWMutex
}

// New creates a Store that persists data to a single file inside dir.
// The directory is created if it does not exist.
func New(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("json store: mkdir %s: %w", dir, err)
	}
	return &Store{file: filepath.Join(dir, dbFile)}, nil
}

// Streams returns a StreamRepository backed by this Store.
func (s *Store) Streams() store.StreamRepository { return &streamRepo{s} }

// Templates returns a TemplateRepository backed by this Store.
func (s *Store) Templates() store.TemplateRepository { return &templateRepo{s} }

// Recordings returns a RecordingRepository backed by this Store.
func (s *Store) Recordings() store.RecordingRepository { return &recordingRepo{s} }

// Hooks returns a HookRepository backed by this Store.
func (s *Store) Hooks() store.HookRepository { return &hookRepo{s} }

// GlobalConfig returns a GlobalConfigRepository backed by this Store.
func (s *Store) GlobalConfig() store.GlobalConfigRepository { return &globalConfigRepo{s} }

// VOD returns a VODMountRepository backed by this Store.
func (s *Store) VOD() store.VODMountRepository { return &vodMountRepo{s} }

// --- internal helpers ---

// readDB reads and deserialises the data file. Nil maps are initialised.
// Caller must hold at least s.mu.RLock.
func (s *Store) readDB() (db, error) {
	data, err := os.ReadFile(s.file)
	if os.IsNotExist(err) {
		return emptyDB(), nil
	}
	if err != nil {
		return db{}, fmt.Errorf("json store: read: %w", err)
	}
	var d db
	if err := json.Unmarshal(data, &d); err != nil {
		return db{}, fmt.Errorf("json store: unmarshal: %w", err)
	}
	if d.Streams == nil {
		d.Streams = make(map[string]*domain.Stream)
	}
	if d.Templates == nil {
		d.Templates = make(map[string]*domain.Template)
	}
	if d.Recordings == nil {
		d.Recordings = make(map[string]*domain.Recording)
	}
	if d.Hooks == nil {
		d.Hooks = make(map[string]*domain.Hook)
	}
	if d.VOD == nil {
		d.VOD = make(map[string]*domain.VODMount)
	}
	return d, nil
}

// writeDB marshals and atomically writes the data file.
// Caller must hold s.mu.Lock.
func (s *Store) writeDB(d db) error {
	data, err := json.MarshalIndent(d, "", "  ")
	if err != nil {
		return fmt.Errorf("json store: marshal: %w", err)
	}
	tmp := s.file + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return fmt.Errorf("json store: write tmp: %w", err)
	}
	if err := os.Rename(tmp, s.file); err != nil {
		return fmt.Errorf("json store: rename: %w", err)
	}
	return nil
}

// readAll reads the full database under a read lock and passes it to fn.
func (s *Store) readAll(fn func(db) error) error {
	s.mu.RLock()
	defer s.mu.RUnlock()
	d, err := s.readDB()
	if err != nil {
		return err
	}
	return fn(d)
}

// modify performs an atomic read-modify-write under a write lock.
func (s *Store) modify(fn func(*db) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, err := s.readDB()
	if err != nil {
		return err
	}
	if err := fn(&d); err != nil {
		return err
	}
	return s.writeDB(d)
}

func emptyDB() db {
	return db{
		Streams:    make(map[string]*domain.Stream),
		Templates:  make(map[string]*domain.Template),
		Recordings: make(map[string]*domain.Recording),
		Hooks:      make(map[string]*domain.Hook),
		VOD:        make(map[string]*domain.VODMount),
	}
}

// --- StreamRepository ---

type streamRepo struct{ s *Store }

// Save implements store.StreamRepository.
func (r *streamRepo) Save(_ context.Context, stream *domain.Stream) error {
	return r.s.modify(func(d *db) error {
		d.Streams[string(stream.Code)] = stream
		return nil
	})
}

// FindByCode implements store.StreamRepository.
func (r *streamRepo) FindByCode(_ context.Context, code domain.StreamCode) (*domain.Stream, error) {
	var result *domain.Stream
	err := r.s.readAll(func(d db) error {
		s, ok := d.Streams[string(code)]
		if !ok {
			return fmt.Errorf("stream %s: %w", code, store.ErrNotFound)
		}
		result = s
		return nil
	})
	return result, err
}

// List implements store.StreamRepository.
func (r *streamRepo) List(_ context.Context, _ store.StreamFilter) ([]*domain.Stream, error) {
	var result []*domain.Stream
	err := r.s.readAll(func(d db) error {
		result = make([]*domain.Stream, 0, len(d.Streams))
		for _, s := range d.Streams {
			result = append(result, s)
		}
		slices.SortFunc(result, func(a, b *domain.Stream) int {
			if a.Code < b.Code {
				return -1
			}
			if a.Code > b.Code {
				return 1
			}
			return 0
		})
		return nil
	})
	return result, err
}

// Delete implements store.StreamRepository.
func (r *streamRepo) Delete(_ context.Context, code domain.StreamCode) error {
	return r.s.modify(func(d *db) error {
		delete(d.Streams, string(code))
		return nil
	})
}

// --- TemplateRepository ---

type templateRepo struct{ s *Store }

// Save implements store.TemplateRepository.
func (r *templateRepo) Save(_ context.Context, tpl *domain.Template) error {
	return r.s.modify(func(d *db) error {
		d.Templates[string(tpl.Code)] = tpl
		return nil
	})
}

// FindByCode implements store.TemplateRepository.
func (r *templateRepo) FindByCode(_ context.Context, code domain.TemplateCode) (*domain.Template, error) {
	var result *domain.Template
	err := r.s.readAll(func(d db) error {
		t, ok := d.Templates[string(code)]
		if !ok {
			return fmt.Errorf("template %s: %w", code, store.ErrNotFound)
		}
		result = t
		return nil
	})
	return result, err
}

// List implements store.TemplateRepository.
func (r *templateRepo) List(_ context.Context) ([]*domain.Template, error) {
	var result []*domain.Template
	err := r.s.readAll(func(d db) error {
		result = make([]*domain.Template, 0, len(d.Templates))
		for _, t := range d.Templates {
			result = append(result, t)
		}
		slices.SortFunc(result, func(a, b *domain.Template) int {
			if a.Code < b.Code {
				return -1
			}
			if a.Code > b.Code {
				return 1
			}
			return 0
		})
		return nil
	})
	return result, err
}

// Delete implements store.TemplateRepository.
func (r *templateRepo) Delete(_ context.Context, code domain.TemplateCode) error {
	return r.s.modify(func(d *db) error {
		delete(d.Templates, string(code))
		return nil
	})
}

// --- RecordingRepository ---

type recordingRepo struct{ s *Store }

// Save implements store.RecordingRepository.
func (r *recordingRepo) Save(_ context.Context, rec *domain.Recording) error {
	return r.s.modify(func(d *db) error {
		d.Recordings[string(rec.ID)] = rec
		return nil
	})
}

// FindByID implements store.RecordingRepository.
func (r *recordingRepo) FindByID(_ context.Context, id domain.RecordingID) (*domain.Recording, error) {
	var result *domain.Recording
	err := r.s.readAll(func(d db) error {
		rec, ok := d.Recordings[string(id)]
		if !ok {
			return fmt.Errorf("recording %s: %w", id, store.ErrNotFound)
		}
		result = rec
		return nil
	})
	return result, err
}

// ListByStream implements store.RecordingRepository.
func (r *recordingRepo) ListByStream(_ context.Context, streamCode domain.StreamCode) ([]*domain.Recording, error) {
	var result []*domain.Recording
	err := r.s.readAll(func(d db) error {
		result = make([]*domain.Recording, 0)
		for _, rec := range d.Recordings {
			if rec.StreamCode == streamCode {
				result = append(result, rec)
			}
		}
		return nil
	})
	return result, err
}

// Delete implements store.RecordingRepository.
func (r *recordingRepo) Delete(_ context.Context, id domain.RecordingID) error {
	return r.s.modify(func(d *db) error {
		delete(d.Recordings, string(id))
		return nil
	})
}

// --- HookRepository ---

type hookRepo struct{ s *Store }

// Save implements store.HookRepository.
func (r *hookRepo) Save(_ context.Context, hook *domain.Hook) error {
	return r.s.modify(func(d *db) error {
		d.Hooks[string(hook.ID)] = hook
		return nil
	})
}

// FindByID implements store.HookRepository.
func (r *hookRepo) FindByID(_ context.Context, id domain.HookID) (*domain.Hook, error) {
	var result *domain.Hook
	err := r.s.readAll(func(d db) error {
		h, ok := d.Hooks[string(id)]
		if !ok {
			return fmt.Errorf("hook %s: %w", id, store.ErrNotFound)
		}
		result = h
		return nil
	})
	return result, err
}

// List implements store.HookRepository.
func (r *hookRepo) List(_ context.Context) ([]*domain.Hook, error) {
	var result []*domain.Hook
	err := r.s.readAll(func(d db) error {
		result = make([]*domain.Hook, 0, len(d.Hooks))
		for _, h := range d.Hooks {
			result = append(result, h)
		}
		return nil
	})
	return result, err
}

// Delete implements store.HookRepository.
func (r *hookRepo) Delete(_ context.Context, id domain.HookID) error {
	return r.s.modify(func(d *db) error {
		delete(d.Hooks, string(id))
		return nil
	})
}

// --- VODMountRepository ---

type vodMountRepo struct{ s *Store }

// Save implements store.VODMountRepository.
func (r *vodMountRepo) Save(_ context.Context, mount *domain.VODMount) error {
	return r.s.modify(func(d *db) error {
		d.VOD[string(mount.Name)] = mount
		return nil
	})
}

// FindByName implements store.VODMountRepository.
func (r *vodMountRepo) FindByName(_ context.Context, name domain.VODName) (*domain.VODMount, error) {
	var result *domain.VODMount
	err := r.s.readAll(func(d db) error {
		m, ok := d.VOD[string(name)]
		if !ok {
			return fmt.Errorf("vod mount %s: %w", name, store.ErrNotFound)
		}
		result = m
		return nil
	})
	return result, err
}

// List implements store.VODMountRepository.
func (r *vodMountRepo) List(_ context.Context) ([]*domain.VODMount, error) {
	var result []*domain.VODMount
	err := r.s.readAll(func(d db) error {
		result = make([]*domain.VODMount, 0, len(d.VOD))
		for _, m := range d.VOD {
			result = append(result, m)
		}
		slices.SortFunc(result, func(a, b *domain.VODMount) int {
			if a.Name < b.Name {
				return -1
			}
			if a.Name > b.Name {
				return 1
			}
			return 0
		})
		return nil
	})
	return result, err
}

// Delete implements store.VODMountRepository.
func (r *vodMountRepo) Delete(_ context.Context, name domain.VODName) error {
	return r.s.modify(func(d *db) error {
		delete(d.VOD, string(name))
		return nil
	})
}
