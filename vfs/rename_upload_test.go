package vfs

import (
	"context"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rclone/rclone/fs"
	"github.com/rclone/rclone/fstest"
	"github.com/rclone/rclone/vfs/vfscommon"
	"github.com/stretchr/testify/require"
)

type renameUploadFs struct {
	fs.Fs
	features fs.Features
}

func (f *renameUploadFs) Features() *fs.Features { return &f.features }

type renameUploadContext struct {
	context.Context
	waiting chan struct{}
	once    sync.Once
}

func (ctx *renameUploadContext) Done() <-chan struct{} {
	ctx.once.Do(func() { close(ctx.waiting) })
	return ctx.Context.Done()
}

func TestRenameWhileCacheUploadFinalizes(t *testing.T) {
	if *fstest.RemoteName != "" {
		t.Skip("requires the default local backend")
	}
	const oldData, newData = "old", "replacement bytes"
	const target, temporary = "target", ".target.tmp"
	const timeout = 10 * time.Second
	ctx := context.Background()
	r := fstest.NewRun(t)
	r.WriteObject(ctx, target, oldData, t1)
	backend := &renameUploadFs{Fs: r.Fremote, features: *r.Fremote.Features()}
	move := backend.features.Move
	require.NotNil(t, move)
	require.True(t, backend.features.PartialUploads)
	started := make(chan struct{})
	resume := make(chan struct{})
	unblock := sync.OnceFunc(func() { close(resume) })
	backend.features.Move = func(ctx context.Context, src fs.Object, remote string) (fs.Object, error) {
		if remote == temporary {
			close(started)
			<-resume
		}
		return move(ctx, src, remote)
	}
	opt := vfscommon.Opt
	opt.CacheMode = vfscommon.CacheModeWrites
	opt.WriteBack = 0
	v := New(ctx, backend, &opt)
	t.Cleanup(func() { cleanupVFS(t, v) })

	h, err := v.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	require.NoError(t, err)
	fh := h.(*RWFileHandle)
	n, err := fh.WriteAt([]byte(newData), 0)
	require.NoError(t, err)
	require.Equal(t, len(newData), n)
	require.NoError(t, fh.Flush())

	released := make(chan struct{})
	var releaseErr error
	go func() {
		defer close(released)
		releaseErr = fh.Release()
	}()
	wait := func(done <-chan struct{}, operation string) {
		t.Helper()
		select {
		case <-done:
		case <-time.After(timeout):
			t.Fatalf("timed out waiting for %s", operation)
		}
	}
	t.Cleanup(func() { unblock(); wait(released, "release") })
	wait(started, "upload finalization")

	renameCtx := &renameUploadContext{Context: ctx, waiting: make(chan struct{})}
	renamed := make(chan struct{})
	var renameErr error
	go func() {
		defer close(renamed)
		renameErr = fh.file.rename(renameCtx, v.root, target)
	}()
	t.Cleanup(func() { unblock(); wait(renamed, "rename") })
	select {
	case <-renameCtx.waiting:
	case <-renamed:
	case <-time.After(timeout):
		t.Fatal("rename did not reach upload coordination")
	}
	unblock()
	wait(released, "release")
	wait(renamed, "rename")
	require.NoError(t, releaseErr)
	require.NoError(t, renameErr)

	obj, err := r.Fremote.NewObject(ctx, target)
	require.NoError(t, err)
	in, err := obj.Open(ctx)
	require.NoError(t, err)
	data, err := io.ReadAll(in)
	closeErr := in.Close()
	require.NoError(t, err)
	require.NoError(t, closeErr)
	require.Equal(t, newData, string(data))
	_, err = r.Fremote.NewObject(ctx, temporary)
	require.ErrorIs(t, err, fs.ErrorObjectNotFound)
}
