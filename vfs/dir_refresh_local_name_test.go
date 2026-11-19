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
	"github.com/stretchr/testify/require"
)

const refreshLocalName = "child/.replace.tmp"
const refreshLocalData = "replacement bytes"

type refreshListFs struct {
	fs.Fs
	omit    bool
	omitted int
}

func (f *refreshListFs) List(ctx context.Context, dir string) (fs.DirEntries, error) {
	entries, err := f.Fs.List(ctx, dir)
	if err != nil || !f.omit {
		return entries, err
	}
	filtered := make(fs.DirEntries, 0, len(entries))
	for _, entry := range entries {
		if entry.Remote() == refreshLocalName {
			f.omitted++
			continue
		}
		filtered = append(filtered, entry)
	}
	return filtered, nil
}

func TestRefreshPreservesLocalName(t *testing.T) {
	if *fstest.RemoteName != "" {
		t.Skip("requires the default local backend")
	}
	ctx := context.Background()
	setup := func(t *testing.T, mode vfscommon.CacheMode) (*VFS, *Dir, *refreshListFs) {
		t.Helper()
		r := fstest.NewRun(t)
		require.NoError(t, r.Fremote.Mkdir(ctx, "child"))
		backend := &refreshListFs{Fs: r.Fremote}
		opt := vfscommon.Opt
		opt.CacheMode = mode
		opt.WriteBack = 0
		opt.PollInterval = 0
		opt.DirCacheTime = fs.Duration(time.Hour)
		v := New(ctx, backend, &opt)
		t.Cleanup(func() { cleanupVFS(t, v) })
		node, err := v.Stat("child")
		require.NoError(t, err)
		return v, node.(*Dir), backend
	}
	write := func(t *testing.T, dir *Dir) (*File, Handle) {
		t.Helper()
		flags := os.O_CREATE | os.O_WRONLY | os.O_TRUNC
		file, err := dir.Create(".replace.tmp", flags)
		require.NoError(t, err)
		h, err := file.Open(flags)
		require.NoError(t, err)
		t.Cleanup(func() { require.NoError(t, h.Release()) })
		n, err := h.Write([]byte(refreshLocalData))
		require.NoError(t, err)
		require.Equal(t, len(refreshLocalData), n)
		require.NoError(t, h.Flush())
		return file, h
	}
	read := func(t *testing.T, in io.ReadCloser) {
		t.Helper()
		data, err := io.ReadAll(in)
		closeErr := in.Close()
		require.NoError(t, err)
		require.NoError(t, closeErr)
		require.Equal(t, refreshLocalData, string(data))
	}
	t.Run("uploaded entry omitted from list", func(t *testing.T) {
		v, dir, backend := setup(t, vfscommon.CacheModeMinimal)
		file, h := write(t, dir)
		require.NoError(t, h.Release())
		obj, err := backend.NewObject(ctx, refreshLocalName)
		require.NoError(t, err)
		in, err := obj.Open(ctx)
		require.NoError(t, err)
		read(t, in)

		backend.omit = true
		require.NoError(t, dir.readDir())
		require.Equal(t, 1, backend.omitted)
		nodes, err := dir.ReadDirAll()
		require.NoError(t, err)
		require.Len(t, nodes, 1, "refresh dropped the uploaded name")
		require.Same(t, file, nodes[0])
		require.NoError(t, v.Rename(refreshLocalName, "child/target"))
		obj, err = backend.NewObject(ctx, "child/target")
		require.NoError(t, err)
		in, err = obj.Open(ctx)
		require.NoError(t, err)
		read(t, in)
	})
	t.Run("create through retained directory after ancestor forget", func(t *testing.T) {
		v, dir, backend := setup(t, vfscommon.CacheModeWrites)
		require.False(t, v.root.ForgetAll())
		file, _ := write(t, dir)
		_, err := backend.NewObject(ctx, refreshLocalName)
		require.ErrorIs(t, err, fs.ErrorObjectNotFound)
		node, err := v.Stat(refreshLocalName)
		require.NoError(t, err, "fresh traversal lost the pending name")
		require.Same(t, file, node)
		h, err := node.Open(os.O_RDONLY)
		require.NoError(t, err)
		read(t, h)
	})
}
