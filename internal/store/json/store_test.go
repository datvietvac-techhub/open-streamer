package json_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/datvietvac-techhub/open-streamer/internal/domain"
	"github.com/datvietvac-techhub/open-streamer/internal/store"
	jsonstore "github.com/datvietvac-techhub/open-streamer/internal/store/json"
	"github.com/datvietvac-techhub/open-streamer/internal/store/storetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newStore(t *testing.T) *jsonstore.Store {
	t.Helper()
	s, err := jsonstore.New(t.TempDir())
	require.NoError(t, err)
	return s
}

// --- StreamRepository ---

func TestJSONStreamRepo_SaveAndFindByCode(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	repo := s.Streams()

	want := storetest.NewFullStream("teststreamA")
	require.NoError(t, repo.Save(ctx, want))

	got, err := repo.FindByCode(ctx, "teststreamA")
	require.NoError(t, err)

	assert.Equal(t, want.Code, got.Code)
	assert.Equal(t, want.Name, got.Name)
	assert.Equal(t, want.Description, got.Description)
	assert.Equal(t, want.Tags, got.Tags)
	assert.Equal(t, want.StreamKey, got.StreamKey)
	assert.Equal(t, want.Disabled, got.Disabled)

	require.Len(t, got.Inputs, 2)
	assert.Equal(t, want.Inputs[0].URL, got.Inputs[0].URL)
	assert.Equal(t, want.Inputs[0].Priority, got.Inputs[0].Priority)
	assert.Equal(t, want.Inputs[0].Headers, got.Inputs[0].Headers)
	assert.Equal(t, want.Inputs[0].Params, got.Inputs[0].Params)
	assert.Equal(t, want.Inputs[0].Net.TimeoutSec, got.Inputs[0].Net.TimeoutSec)
	assert.Equal(t, want.Inputs[0].Net.InsecureTLS, got.Inputs[0].Net.InsecureTLS)

	require.NotNil(t, got.Transcoder)
	assert.Equal(t, want.Transcoder.Video.Copy, got.Transcoder.Video.Copy)
	require.Len(t, got.Transcoder.Video.Profiles, 2)
	p0 := got.Transcoder.Video.Profiles[0]
	assert.Equal(t, want.Transcoder.Video.Profiles[0].Width, p0.Width)
	assert.Equal(t, want.Transcoder.Video.Profiles[0].Height, p0.Height)
	assert.Equal(t, want.Transcoder.Video.Profiles[0].Bitrate, p0.Bitrate)
	assert.Equal(t, want.Transcoder.Video.Profiles[0].MaxBitrate, p0.MaxBitrate)
	assert.Equal(t, want.Transcoder.Video.Profiles[0].Framerate, p0.Framerate)
	assert.Equal(t, want.Transcoder.Video.Profiles[0].KeyframeInterval, p0.KeyframeInterval)
	assert.Equal(t, want.Transcoder.Video.Profiles[0].Codec, p0.Codec)
	assert.Equal(t, want.Transcoder.Video.Profiles[0].Preset, p0.Preset)
	assert.Equal(t, want.Transcoder.Video.Profiles[0].Profile, p0.Profile)
	assert.Equal(t, want.Transcoder.Video.Profiles[0].Level, p0.Level)

	assert.Equal(t, want.Transcoder.Audio.Copy, got.Transcoder.Audio.Copy)
	assert.Equal(t, want.Transcoder.Audio.Codec, got.Transcoder.Audio.Codec)
	assert.Equal(t, want.Transcoder.Audio.Bitrate, got.Transcoder.Audio.Bitrate)
	assert.Equal(t, want.Transcoder.Audio.SampleRate, got.Transcoder.Audio.SampleRate)
	assert.Equal(t, want.Transcoder.Audio.Channels, got.Transcoder.Audio.Channels)
	assert.Equal(t, want.Transcoder.Audio.Language, got.Transcoder.Audio.Language)
	assert.Equal(t, want.Transcoder.Audio.Normalize, got.Transcoder.Audio.Normalize)

	assert.Equal(t, want.Transcoder.Decoder.Name, got.Transcoder.Decoder.Name)
	assert.Equal(t, want.Transcoder.Global.HW, got.Transcoder.Global.HW)
	assert.Equal(t, want.Transcoder.Global.FPS, got.Transcoder.Global.FPS)
	assert.Equal(t, want.Transcoder.Global.GOP, got.Transcoder.Global.GOP)
	assert.Equal(t, want.Transcoder.Global.DeviceID, got.Transcoder.Global.DeviceID)

	assert.Equal(t, want.Protocols, got.Protocols)

	require.Len(t, got.Push, 1)
	assert.Equal(t, want.Push[0].URL, got.Push[0].URL)
	assert.Equal(t, want.Push[0].Enabled, got.Push[0].Enabled)
	assert.Equal(t, want.Push[0].TimeoutSec, got.Push[0].TimeoutSec)
	assert.Equal(t, want.Push[0].RetryTimeoutSec, got.Push[0].RetryTimeoutSec)
	assert.Equal(t, want.Push[0].Limit, got.Push[0].Limit)
	assert.Equal(t, want.Push[0].Comment, got.Push[0].Comment)

	require.NotNil(t, got.DVR)
	assert.Equal(t, want.DVR.Enabled, got.DVR.Enabled)
	assert.Equal(t, want.DVR.RetentionSec, got.DVR.RetentionSec)
	assert.Equal(t, want.DVR.SegmentDuration, got.DVR.SegmentDuration)
	assert.Equal(t, want.DVR.StoragePath, got.DVR.StoragePath)
	assert.Equal(t, want.DVR.MaxSizeGB, got.DVR.MaxSizeGB)
}

func TestJSONStreamRepo_FindByCode_NotFound(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Streams()

	_, err := repo.FindByCode(ctx, "nonexistent")
	require.Error(t, err)
	assert.True(t, errors.Is(err, store.ErrNotFound))
}

func TestJSONStreamRepo_List(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Streams()

	s1 := storetest.NewFullStream("stream1")
	s2 := storetest.NewFullStream("stream2")

	require.NoError(t, repo.Save(ctx, s1))
	require.NoError(t, repo.Save(ctx, s2))

	all, err := repo.List(ctx, store.StreamFilter{})
	require.NoError(t, err)
	assert.Len(t, all, 2)
}

func TestJSONStreamRepo_Update(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Streams()

	s := storetest.NewFullStream("update_me")
	require.NoError(t, repo.Save(ctx, s))

	s.Name = "Updated Name"
	require.NoError(t, repo.Save(ctx, s))

	got, err := repo.FindByCode(ctx, "update_me")
	require.NoError(t, err)
	assert.Equal(t, "Updated Name", got.Name)
}

func TestJSONStreamRepo_Delete(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Streams()

	s := storetest.NewFullStream("delete_me")
	require.NoError(t, repo.Save(ctx, s))

	require.NoError(t, repo.Delete(ctx, "delete_me"))

	_, err := repo.FindByCode(ctx, "delete_me")
	require.Error(t, err)
	assert.True(t, errors.Is(err, store.ErrNotFound))
}

// --- TemplateRepository ---

func TestJSONTemplateRepo_SaveAndFindByCode(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Templates()

	want := storetest.NewFullTemplate("profile_a")
	require.NoError(t, repo.Save(ctx, want))

	got, err := repo.FindByCode(ctx, "profile_a")
	require.NoError(t, err)
	assert.Equal(t, want.Code, got.Code)
	assert.Equal(t, want.Name, got.Name)
	assert.Equal(t, want.Description, got.Description)
	assert.Equal(t, want.Protocols, got.Protocols)
	require.Len(t, got.Push, 1)
	assert.Equal(t, want.Push[0].URL, got.Push[0].URL)
	require.NotNil(t, got.DVR)
	assert.Equal(t, want.DVR.RetentionSec, got.DVR.RetentionSec)
}

func TestJSONTemplateRepo_FindByCode_NotFound(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Templates()
	_, err := repo.FindByCode(ctx, "nope")
	require.Error(t, err)
	assert.True(t, errors.Is(err, store.ErrNotFound))
}

func TestJSONTemplateRepo_ListSorted(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Templates()

	require.NoError(t, repo.Save(ctx, storetest.NewFullTemplate("b_template")))
	require.NoError(t, repo.Save(ctx, storetest.NewFullTemplate("a_template")))

	all, err := repo.List(ctx)
	require.NoError(t, err)
	require.Len(t, all, 2)
	assert.Equal(t, domain.TemplateCode("a_template"), all[0].Code, "List must be sorted by code asc")
	assert.Equal(t, domain.TemplateCode("b_template"), all[1].Code)
}

func TestJSONTemplateRepo_Delete(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Templates()

	tpl := storetest.NewFullTemplate("delete_me")
	require.NoError(t, repo.Save(ctx, tpl))
	require.NoError(t, repo.Delete(ctx, "delete_me"))

	_, err := repo.FindByCode(ctx, "delete_me")
	assert.True(t, errors.Is(err, store.ErrNotFound))
}

// --- RecordingRepository ---

func TestJSONRecordingRepo_SaveAndFindByID(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Recordings()

	want := storetest.NewFullRecording("rec1", "stream1")
	require.NoError(t, repo.Save(ctx, want))

	got, err := repo.FindByID(ctx, "rec1")
	require.NoError(t, err)

	assert.Equal(t, want.ID, got.ID)
	assert.Equal(t, want.StreamCode, got.StreamCode)
	assert.Equal(t, want.SegmentDir, got.SegmentDir)
	assert.Equal(t, want.StartedAt.UTC(), got.StartedAt.UTC())
	require.NotNil(t, got.StoppedAt)
	assert.Equal(t, want.StoppedAt.UTC(), got.StoppedAt.UTC())
}

func TestJSONRecordingRepo_FindByID_NotFound(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Recordings()

	_, err := repo.FindByID(ctx, "noexist")
	require.Error(t, err)
	assert.True(t, errors.Is(err, store.ErrNotFound))
}

func TestJSONRecordingRepo_ListByStream(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Recordings()

	r1 := storetest.NewFullRecording("recA", "stream_alpha")
	r2 := storetest.NewFullRecording("recB", "stream_alpha")
	r3 := storetest.NewFullRecording("recC", "stream_beta")

	require.NoError(t, repo.Save(ctx, r1))
	require.NoError(t, repo.Save(ctx, r2))
	require.NoError(t, repo.Save(ctx, r3))

	list, err := repo.ListByStream(ctx, "stream_alpha")
	require.NoError(t, err)
	assert.Len(t, list, 2)

	other, err := repo.ListByStream(ctx, "stream_beta")
	require.NoError(t, err)
	assert.Len(t, other, 1)
	assert.Equal(t, domain.RecordingID("recC"), other[0].ID)
}

func TestJSONRecordingRepo_Delete(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Recordings()

	r := storetest.NewFullRecording("del_rec", "stream1")
	require.NoError(t, repo.Save(ctx, r))
	require.NoError(t, repo.Delete(ctx, "del_rec"))

	_, err := repo.FindByID(ctx, "del_rec")
	require.Error(t, err)
	assert.True(t, errors.Is(err, store.ErrNotFound))
}

// --- HookRepository ---

func TestJSONHookRepo_SaveAndFindByID(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Hooks()

	want := storetest.NewFullHook("hook1")
	require.NoError(t, repo.Save(ctx, want))

	got, err := repo.FindByID(ctx, "hook1")
	require.NoError(t, err)

	assert.Equal(t, want.ID, got.ID)
	assert.Equal(t, want.Name, got.Name)
	assert.Equal(t, want.Type, got.Type)
	assert.Equal(t, want.Target, got.Target)
	assert.Equal(t, want.Secret, got.Secret)
	assert.Equal(t, want.EventTypes, got.EventTypes)
	assert.Equal(t, want.Enabled, got.Enabled)
	assert.Equal(t, want.MaxRetries, got.MaxRetries)
	assert.Equal(t, want.TimeoutSec, got.TimeoutSec)
}

func TestJSONHookRepo_FindByID_NotFound(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Hooks()

	_, err := repo.FindByID(ctx, "ghost")
	require.Error(t, err)
	assert.True(t, errors.Is(err, store.ErrNotFound))
}

func TestJSONHookRepo_List(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Hooks()

	require.NoError(t, repo.Save(ctx, storetest.NewFullHook("h1")))
	require.NoError(t, repo.Save(ctx, storetest.NewFullHook("h2")))

	list, err := repo.List(ctx)
	require.NoError(t, err)
	assert.Len(t, list, 2)
}

func TestJSONHookRepo_Delete(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Hooks()

	require.NoError(t, repo.Save(ctx, storetest.NewFullHook("del_hook")))
	require.NoError(t, repo.Delete(ctx, "del_hook"))

	_, err := repo.FindByID(ctx, "del_hook")
	require.Error(t, err)
	assert.True(t, errors.Is(err, store.ErrNotFound))
}

// --- Concurrent access ---

func TestJSONStreamRepo_ConcurrentSaveAndFind(t *testing.T) {
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

func TestJSONRecordingRepo_ConcurrentSaveAndFind(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Recordings()

	const workers = 10
	var wg sync.WaitGroup

	for i := range workers {
		id := domain.RecordingID(fmt.Sprintf("rec_%d", i))
		r := storetest.NewFullRecording(id, "stream1")
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, repo.Save(ctx, r))
			got, err := repo.FindByID(ctx, id)
			require.NoError(t, err)
			assert.Equal(t, id, got.ID)
		}()
	}
	wg.Wait()

	all, err := repo.ListByStream(ctx, "stream1")
	require.NoError(t, err)
	assert.Len(t, all, workers)
}

func TestJSONHookRepo_ConcurrentSaveAndFind(t *testing.T) {
	ctx := context.Background()
	repo := newStore(t).Hooks()

	const workers = 10
	var wg sync.WaitGroup

	for i := range workers {
		id := domain.HookID(fmt.Sprintf("hook_%d", i))
		h := storetest.NewFullHook(id)
		wg.Add(1)
		go func() {
			defer wg.Done()
			require.NoError(t, repo.Save(ctx, h))
			got, err := repo.FindByID(ctx, id)
			require.NoError(t, err)
			assert.Equal(t, id, got.ID)
		}()
	}
	wg.Wait()

	all, err := repo.List(ctx)
	require.NoError(t, err)
	assert.Len(t, all, workers)
}
