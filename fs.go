package scioredb

import (
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

type File interface {
	ReadAt(b []byte, off int64) (n int, err error)
	WriteAt(b []byte, off int64) (n int, err error)
	Sync() error
	Size() (int64, error)
	Close() error
}

type FS interface {
	Open(name string) (File, error)
}

type osFile struct {
	*os.File
}

func (f *osFile) Size() (int64, error) {
	info, err := f.Stat()
	if err != nil {
		return 0, err
	}
	return info.Size(), nil
}

type DirFS string

func (dir DirFS) Open(name string) (File, error) {
	if dir == "" {
		return nil, errors.New("DirFS empty root")
	}
	filename := filepath.Join(string(dir), filepath.Base(name))
	f, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, &fs.PathError{Op: "open", Path: name, Err: err}
	}
	return &osFile{f}, nil
}
