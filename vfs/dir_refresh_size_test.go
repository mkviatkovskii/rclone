package vfs

import (
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type refreshSizeFs struct {
	fs.Fs
	listing fs.DirEntries
	lists   int
	points  int
}

func (f *refreshSizeFs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	f.lists++
	if dir == "" && f.listing != nil {
		return f.listing, nil
	}
	return f.Fs.List(ctx, dir)
}

func (f *refreshSizeFs) NewObject(ctx context.Context, remote string) (fs.Object, error) {
	f.points++
	return f.Fs.NewObject(ctx, remote)
}

func TestRefreshConfirmsLocallyPublishedSize(t *testing.T) {
	if *fstest.RemoteName != "" {
		t.Skip("requires the default local backend")
	}
	const target, temporary, original = "target", ".target.tmp", "old"
	for _, tc := range []struct {
		name, replacement string
		local             bool
		wantPoints        int
		wantSize          int64
	}{
		{"changed size", "replacement bytes", true, 1, int64(len("replacement bytes"))},
		{"same size", "new", true, 0, int64(len(original))},
		{"no local publication", "external bytes", false, 0, int64(len(original))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			r := fstest.NewRun(t)
			r.WriteObject(ctx, target, original, t1)
			stale, err := r.Fremote.List(ctx, "")
			require.NoError(t, err)
			require.Len(t, stale, 1)
			backend := &refreshSizeFs{Fs: r.Fremote}
			opt := vfscommon.Opt
			opt.CacheMode = vfscommon.CacheModeWrites
			opt.WriteBack = 0
			opt.PollInterval = 0
			opt.DirCacheTime = fs.Duration(time.Hour)
			v := New(ctx, backend, &opt)
			t.Cleanup(func() { cleanupVFS(t, v) })
			_, err = v.Stat(target)
			require.NoError(t, err)

			if tc.local {
				h, err := v.OpenFile(temporary, os.O_CREATE|os.O_WRONLY|os.O_EXCL, 0600)
				require.NoError(t, err)
				t.Cleanup(func() { assert.NoError(t, h.Release()) })
				n, err := h.Write([]byte(tc.replacement))
				require.NoError(t, err)
				require.Equal(t, len(tc.replacement), n)
				require.NoError(t, h.Release())
				require.NoError(t, v.Rename(temporary, target))
			} else {
				r.WriteObject(ctx, target, tc.replacement, t2)
			}
			obj, err := r.Fremote.NewObject(ctx, target)
			require.NoError(t, err)
			in, err := obj.Open(ctx)
			require.NoError(t, err)
			data, readErr := io.ReadAll(in)
			closeErr := in.Close()
			require.NoError(t, readErr)
			require.NoError(t, closeErr)
			require.Equal(t, tc.replacement, string(data))
			require.Equal(t, int64(len(original)), stale[0].Size())

			refresh := func(label string, wantPoints int, wantSize int64) {
				t.Helper()
				lists, points := backend.lists, backend.points
				require.NoError(t, v.root.readDir())
				listDelta, pointDelta := backend.lists-lists, backend.points-points
				t.Logf("%s: %d lists, %d point lookups", label, listDelta, pointDelta)
				assert.Equal(t, 1, listDelta, label)
				assert.Equal(t, wantPoints, pointDelta, label)
				node, err := v.Stat(target)
				require.NoError(t, err)
				assert.Equal(t, wantSize, node.Size(), label)
			}
			backend.listing = stale
			refresh("first stale refresh", tc.wantPoints, tc.wantSize)
			refresh("second stale refresh", tc.wantPoints, tc.wantSize)
			backend.listing = nil
			refresh("converged refresh", 0, int64(len(tc.replacement)))
		})
	}
}
