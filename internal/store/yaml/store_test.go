package yaml_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/datvietvac-techhub/open-streamer/config"
	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
	"github.com/datvietvac-techhub/open-streamer/internal/store/storetest"
	yamlstore "github.com/datvietvac-techhub/open-streamer/internal/store/yaml"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newStore(t *testing.T) *yamlstore.Store {
	t.Helper()
	s, err := yamlstore.New(t.TempDir())
	require.NoError(t, err)
	return s
}

// --- StreamRepository ---

func TestYAMLStreamRepo_SaveAndFindByCode(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Streams()

	want := storetest.NewFullStream("teststreamA")
	require.NoError(t, repo.Save(ctx, want))

	got, err := repo.FindByCode(ctx, "teststreamA")
	require.NoError(t, err)
	assert.Equal(t, want.Code, got.Code)
	assert.Equal(t, want.Name, got.Name)
}

func TestYAMLStreamRepo_FindByCode_NotFound(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Streams()

	_, err := repo.FindByCode(ctx, "nonexistent")
	require.Error(t, err)
	assert.True(t, errors.Is(err, store.ErrNotFound))
}

func TestYAMLStreamRepo_List(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Streams()

	require.NoError(t, repo.Save(ctx, storetest.NewFullStream("stream1")))
	require.NoError(t, repo.Save(ctx, storetest.NewFullStream("stream2")))

	all, err := repo.List(ctx, store.StreamFilter{})
	require.NoError(t, err)
	assert.Len(t, all, 2)
}

func TestYAMLStreamRepo_Delete(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Streams()

	require.NoError(t, repo.Save(ctx, storetest.NewFullStream("delete_me")))
	require.NoError(t, repo.Delete(ctx, "delete_me"))

	_, err := repo.FindByCode(ctx, "delete_me")
	require.Error(t, err)
	assert.True(t, errors.Is(err, store.ErrNotFound))
}

// --- TemplateRepository ---

func TestYAMLTemplateRepo_SaveAndFindByCode(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Templates()

	want := storetest.NewFullTemplate("profile_a")
	require.NoError(t, repo.Save(ctx, want))

	got, err := repo.FindByCode(ctx, "profile_a")
	require.NoError(t, err)
	assert.Equal(t, want.Code, got.Code)
	assert.Equal(t, want.Name, got.Name)
	assert.Equal(t, want.Protocols, got.Protocols)
	require.Len(t, got.Push, 1)
	assert.Equal(t, want.Push[0].URL, got.Push[0].URL)
}

func TestYAMLTemplateRepo_Delete(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Templates()

	require.NoError(t, repo.Save(ctx, storetest.NewFullTemplate("delete_me")))
	require.NoError(t, repo.Delete(ctx, "delete_me"))

	_, err := repo.FindByCode(ctx, "delete_me")
	require.Error(t, err)
	assert.True(t, errors.Is(err, store.ErrNotFound))
}

// --- RecordingRepository ---

func TestYAMLRecordingRepo_SaveAndFindByID(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Recordings()

	want := storetest.NewFullRecording("rec1", "stream1")
	require.NoError(t, repo.Save(ctx, want))

	got, err := repo.FindByID(ctx, "rec1")
	require.NoError(t, err)
	assert.Equal(t, want.ID, got.ID)
	assert.Equal(t, want.StreamCode, got.StreamCode)
}

func TestYAMLRecordingRepo_ListByStream(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Recordings()

	require.NoError(t, repo.Save(ctx, storetest.NewFullRecording("recA", "stream_alpha")))
	require.NoError(t, repo.Save(ctx, storetest.NewFullRecording("recB", "stream_alpha")))
	require.NoError(t, repo.Save(ctx, storetest.NewFullRecording("recC", "stream_beta")))

	list, err := repo.ListByStream(ctx, "stream_alpha")
	require.NoError(t, err)
	assert.Len(t, list, 2)
}

// --- HookRepository ---

func TestYAMLHookRepo_SaveAndFindByID(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Hooks()

	want := storetest.NewFullHook("hook1")
	require.NoError(t, repo.Save(ctx, want))

	got, err := repo.FindByID(ctx, "hook1")
	require.NoError(t, err)
	assert.Equal(t, want.ID, got.ID)
	assert.Equal(t, want.Name, got.Name)
}

func TestYAMLHookRepo_Delete(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Hooks()

	require.NoError(t, repo.Save(ctx, storetest.NewFullHook("del_hook")))
	require.NoError(t, repo.Delete(ctx, "del_hook"))

	_, err := repo.FindByID(ctx, "del_hook")
	require.Error(t, err)
	assert.True(t, errors.Is(err, store.ErrNotFound))
}

// --- GlobalConfigRepository ---

func TestYAMLGlobalConfigRepo_GetSet(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).GlobalConfig()

	_, err := repo.Get(ctx)
	require.Error(t, err)
	assert.True(t, errors.Is(err, store.ErrNotFound))

	want := &domain.GlobalConfig{
		Server: &config.ServerConfig{HTTPAddr: ":9090"},
		Log:    &config.LogConfig{Level: "debug", Format: "json"},
	}
	require.NoError(t, repo.Set(ctx, want))

	got, err := repo.Get(ctx)
	require.NoError(t, err)
	assert.Equal(t, want.Server.HTTPAddr, got.Server.HTTPAddr)
	assert.Equal(t, want.Log.Level, got.Log.Level)
	assert.Equal(t, want.Log.Format, got.Log.Format)
	assert.Nil(t, got.Ingestor)
}

// --- Concurrent access ---

func TestYAMLStreamRepo_ConcurrentSaveAndFind(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Streams()

	const workers = 10
	var wg sync.WaitGroup

	for i := range workers {
		code := domain.StreamCode(fmt.Sprintf("concurrent_%d", i))
		s := storetest.NewFullStream(code)
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, repo.Save(ctx, s))
			got, err := repo.FindByCode(ctx, code)
			require.NoError(t, err)
			assert.Equal(t, code, got.Code)
		}()
	}
	wg.Wait()

	all, err := repo.List(ctx, store.StreamFilter{})
	require.NoError(t, err)
	assert.Len(t, all, workers)
}
