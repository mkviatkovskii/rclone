package vfs

import (
	"time"
	"weak"
)

type dirKey struct {
	parent uint64
	leaf   string
}

type dirBinding struct {
	dir    weak.Pointer[Dir]
	timer  *time.Timer
	pinned *Dir
}

func (vfs *VFS) bindDir(d *Dir) *Dir {
	vfs.dirMu.Lock()
	defer vfs.dirMu.Unlock()
	if vfs.ctx.Err() != nil {
		d.retired.Store(true)
		d.cleanupTimer = time.NewTimer(time.Hour)
		d.cleanupTimer.Stop()
		return d
	}
	key := dirKey{leaf: name(d.path)}
	if d.parent != nil {
		key.parent = d.parent.inode
	}
	children := vfs.dirs[key.parent]
	if children == nil {
		children = make(map[string]*dirBinding)
		vfs.dirs[key.parent] = children
	}
	if binding := children[key.leaf]; binding != nil {
		if old := binding.dir.Value(); old != nil && !old.retired.Load() {
			return old
		}
		binding.timer.Stop()
	}
	d.key = key
	ref := weak.Make(d)
	d.cleanupTimer = time.AfterFunc(time.Hour, func() {
		if dir := ref.Value(); dir != nil {
			dir.cacheCleanup()
		}
	})
	children[key.leaf] = &dirBinding{dir: ref, timer: d.cleanupTimer}
	d.resetCleanup()
	return d
}

func (d *Dir) cleanupInterval() time.Duration {
	delay := time.Duration(d.vfs.Opt.DirCacheTime)
	if delay <= 0 {
		return time.Second
	}
	return min(delay, time.Duration(1<<62-1)) * 2
}

func (d *Dir) resetCleanup() {
	if d.retired.Load() || d.vfs.ctx.Err() != nil {
		d.cleanupTimer.Stop()
		return
	}
	d.cleanupTimer.Reset(d.cleanupInterval())
}

func (vfs *VFS) sweepDirs() {
	vfs.dirMu.Lock()
	defer vfs.dirMu.Unlock()
	for parent, children := range vfs.dirs {
		for leaf, binding := range children {
			if binding.dir.Value() == nil {
				binding.timer.Stop()
				delete(children, leaf)
			}
		}
		if len(children) == 0 {
			delete(vfs.dirs, parent)
		}
	}
}

func (vfs *VFS) maintainDirs() {
	defer close(vfs.dirSweepDone)
	interval := max(time.Second, min(time.Duration(vfs.Opt.DirCacheTime), 30*time.Second)*2)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			vfs.sweepDirs()
		case <-vfs.ctx.Done():
			vfs.dirMu.Lock()
			for _, children := range vfs.dirs {
				for _, binding := range children {
					binding.timer.Stop()
				}
			}
			clear(vfs.dirs)
			vfs.dirMu.Unlock()
			return
		}
	}
}

func (vfs *VFS) liveChildren(d *Dir) map[string]*Dir {
	vfs.dirMu.Lock()
	defer vfs.dirMu.Unlock()
	out := make(map[string]*Dir)
	for leaf, binding := range vfs.dirs[d.inode] {
		if child := binding.dir.Value(); child != nil && !child.retired.Load() {
			out[leaf] = child
		} else {
			binding.timer.Stop()
			delete(vfs.dirs[d.inode], leaf)
		}
	}
	return out
}

func (d *Dir) addVirtual(n int32) {
	d.vfs.dirMu.Lock()
	defer d.vfs.dirMu.Unlock()
	if d.retired.Load() {
		return
	}
	for dir := d; dir != nil; dir = dir.parent {
		dir._virtuals.Add(n)
		if binding := d.vfs.dirs[dir.key.parent][dir.key.leaf]; binding != nil {
			if dir.hasVirtual() {
				binding.pinned = dir
			} else {
				binding.pinned = nil
			}
		}
	}
}

func (d *Dir) beginMutation() (func(), error) {
	vfs := d.vfs
	vfs.dirMu.Lock()
	for {
		if vfs.ctx.Err() != nil {
			vfs.dirMu.Unlock()
			return nil, ECLOSED
		}
		blocked := false
		for dir := d; dir != nil; dir = dir.parent {
			if dir.retired.Load() {
				vfs.dirMu.Unlock()
				return nil, ENOENT
			}
			blocked = blocked || dir.removing
		}
		if !blocked {
			break
		}
		changed := vfs.dirChanged
		vfs.dirMu.Unlock()
		select {
		case <-changed:
		case <-vfs.ctx.Done():
			return nil, ECLOSED
		}
		vfs.dirMu.Lock()
	}
	for dir := d; dir != nil; dir = dir.parent {
		dir.pending++
	}
	vfs.dirMu.Unlock()
	return func() {
		vfs.dirMu.Lock()
		for dir := d; dir != nil; dir = dir.parent {
			dir.pending--
		}
		vfs.notifyDirs()
		vfs.dirMu.Unlock()
	}, nil
}

func (vfs *VFS) notifyDirs() {
	close(vfs.dirChanged)
	vfs.dirChanged = make(chan struct{})
}

func (d *Dir) beginRemove() (func(bool), error) {
	vfs := d.vfs
	vfs.dirMu.Lock()
	for {
		if vfs.ctx.Err() != nil {
			vfs.dirMu.Unlock()
			return nil, ECLOSED
		}
		blocked := d.pending != 0
		for dir := d; dir != nil; dir = dir.parent {
			if dir.retired.Load() {
				vfs.dirMu.Unlock()
				return nil, ENOENT
			}
			blocked = blocked || dir.removing
		}
		if !blocked {
			break
		}
		changed := vfs.dirChanged
		vfs.dirMu.Unlock()
		select {
		case <-changed:
		case <-vfs.ctx.Done():
			return nil, ECLOSED
		}
		vfs.dirMu.Lock()
	}
	d.removing = true
	vfs.dirMu.Unlock()
	return func(removed bool) {
		vfs.dirMu.Lock()
		if removed {
			vfs.retireDir(d)
		}
		d.removing = false
		vfs.notifyDirs()
		vfs.dirMu.Unlock()
	}, nil
}

func (vfs *VFS) retireDir(d *Dir) {
	if d.retired.Swap(true) {
		return
	}
	count := d._virtuals.Swap(0)
	for parent := d.parent; parent != nil; parent = parent.parent {
		parent._virtuals.Add(-count)
		if binding := vfs.dirs[parent.key.parent][parent.key.leaf]; binding != nil &&
			!parent.hasVirtual() {
			binding.pinned = nil
		}
	}
	for _, binding := range vfs.dirs[d.inode] {
		if child := binding.dir.Value(); child != nil {
			child._virtuals.Store(0)
			vfs.retireDir(child)
		} else {
			binding.timer.Stop()
		}
	}
	delete(vfs.dirs, d.inode)
	d.cleanupTimer.Stop()
	delete(vfs.dirs[d.key.parent], d.key.leaf)
}

func (d *Dir) reconcileChildren(mv manageVirtuals) {
	for leaf, child := range d.vfs.liveChildren(d) {
		if _, listed := mv[leaf]; listed {
			continue
		}
		d.vfs.dirMu.Lock()
		if child.hasVirtual() || child.pending != 0 || child.removing {
			d.items[leaf] = child
			mv[leaf] = struct{}{}
		} else {
			d.vfs.retireDir(child)
		}
		d.vfs.dirMu.Unlock()
	}
}

func (d *Dir) rebindParent(parent *Dir, leaf string) {
	vfs := d.vfs
	vfs.dirMu.Lock()
	defer vfs.dirMu.Unlock()
	binding := vfs.dirs[d.key.parent][d.key.leaf]
	delete(vfs.dirs[d.key.parent], d.key.leaf)
	virtuals := d._virtuals.Load()
	for old := d.parent; old != nil; old = old.parent {
		old._virtuals.Add(-virtuals)
		old.pending -= d.pending
		if b := vfs.dirs[old.key.parent][old.key.leaf]; b != nil && !old.hasVirtual() {
			b.pinned = nil
		}
	}
	d.parent = parent
	d.key = dirKey{parent: parent.inode, leaf: leaf}
	for next := parent; next != nil; next = next.parent {
		next._virtuals.Add(virtuals)
		next.pending += d.pending
		if b := vfs.dirs[next.key.parent][next.key.leaf]; b != nil && next.hasVirtual() {
			b.pinned = next
		}
	}
	if vfs.dirs[parent.inode] == nil {
		vfs.dirs[parent.inode] = make(map[string]*dirBinding)
	}
	if previous := vfs.dirs[parent.inode][leaf]; previous != nil && previous != binding {
		if old := previous.dir.Value(); old != nil {
			vfs.retireDir(old)
		} else {
			previous.timer.Stop()
		}
	}
	vfs.dirs[parent.inode][leaf] = binding
}
