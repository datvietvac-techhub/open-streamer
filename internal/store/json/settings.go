package json

import (
	"context"
	"fmt"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
)

type globalConfigRepo struct{ s *Store }

// Get implements store.GlobalConfigRepository.
func (r *globalConfigRepo) Get(_ context.Context) (*domain.GlobalConfig, error) {
	var result *domain.GlobalConfig
	err := r.s.readAll(func(d db) error {
		if d.Global == nil {
			return fmt.Errorf("global config: %w", store.ErrNotFound)
		}
		result = d.Global
		return nil
	})
	return result, err
}

// Set implements store.GlobalConfigRepository.
func (r *globalConfigRepo) Set(_ context.Context, cfg *domain.GlobalConfig) error {
	return r.s.modify(func(d *db) error {
		d.Global = cfg
		return nil
	})
}
