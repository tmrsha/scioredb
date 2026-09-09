package scioredb

import (
	"errors"
	"io"
	"io/fs"
	"path/filepath"
	"sync"
	"sync/atomic"
)

type memFile struct {
	name      string
	data      *OffsetBuffer
	mu        sync.RWMutex
	syncCount atomic.Int64
}

func (f *memFile) WriteAt(p []byte, off int64) (int, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := f.data.WriteAt(p, off)
	if err != nil {
		if errors.Is(err, ErrNegativeOffset) {
			return 0, &fs.PathError{Op: "writeat", Path: f.name, Err: ErrNegativeOffset}
		}
		return 0, &fs.PathError{Op: "write", Path: f.name, Err: err}
	}

	return n, nil
}

func (f *memFile) ReadAt(p []byte, off int64) (int, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	n, err := f.data.ReadAt(p, off)
	if err != nil {
		switch {
		case errors.Is(err, io.EOF):
			return n, err
		case errors.Is(err, ErrNegativeOffset):
			return 0, &fs.PathError{Op: "readat", Path: f.name, Err: ErrNegativeOffset}
		default:
			return 0, &fs.PathError{Op: "read", Path: f.name, Err: err}
		}
	}
	return n, nil
}

func (f *memFile) SyncCount() int64 {
	return f.syncCount.Load()
}

func (f *memFile) Size() (size int64, err error) {
	f.mu.RLock()
	size = int64(len(f.data.Bytes()))
	f.mu.RUnlock()
	return
}

type memFileDescriptor struct {
	file   *memFile
	closed atomic.Bool
}

func (f *memFileDescriptor) Name() string {
	return f.file.name
}

func (f *memFileDescriptor) WriteAt(p []byte, off int64) (n int, err error) {
	if f.closed.Load() {
		return 0, &fs.PathError{Op: "writeat", Path: f.file.name, Err: fs.ErrClosed}
	}
	return f.file.WriteAt(p, off)
}

func (f *memFileDescriptor) ReadAt(p []byte, off int64) (n int, err error) {
	if f.closed.Load() {
		return 0, &fs.PathError{Op: "readat", Path: f.file.name, Err: fs.ErrClosed}
	}
	return f.file.ReadAt(p, off)
}

func (f *memFileDescriptor) Sync() error {
	if f.closed.Load() {
		return &fs.PathError{Op: "sync", Path: f.file.name, Err: fs.ErrClosed}
	}
	f.file.syncCount.Add(1)
	return nil
}

func (f *memFileDescriptor) Size() (size int64, err error) {
	if f.closed.Load() {
		return 0, &fs.PathError{Op: "size", Path: f.file.name, Err: fs.ErrClosed}
	}
	return f.file.Size()
}

func (f *memFileDescriptor) Close() error {
	if f.closed.Load() {
		return &fs.PathError{Op: "close", Path: f.file.name, Err: fs.ErrClosed}
	}
	f.closed.Store(true)
	return nil
}

type memFS struct {
	files map[string]*memFile
	mu    sync.RWMutex
}

func MemFS() FS {
	return &memFS{files: make(map[string]*memFile)}
}

func (fsys *memFS) Open(name string) (File, error) {
	filename := filepath.Base(name)
	fsys.mu.RLock()
	f, ok := fsys.files[filename]
	fsys.mu.RUnlock()
	if ok {
		fd := memFileDescriptor{file: f}
		return &fd, nil
	}
	fsys.mu.Lock()
	defer fsys.mu.Unlock()
	if f, ok = fsys.files[filename]; ok {
		fd := memFileDescriptor{file: f}
		return &fd, nil
	}
	f = &memFile{name: filename, data: NewOffsetBuffer(nil)}
	fsys.files[filename] = f
	fd := memFileDescriptor{file: f}
	return &fd, nil
}
