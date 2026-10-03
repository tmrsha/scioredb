package scioredb

import (
	"errors"
	"fmt"
	"sync"
)

type FileManager struct {
	fs        FS
	blockSize int64
	openFiles map[string]File
	isClosed  bool
	mu        sync.RWMutex
}

func NewFileManager(fs FS, blockSize int64) *FileManager {
	return &FileManager{
		fs:        fs,
		blockSize: blockSize,
		openFiles: make(map[string]File),
	}
}

func (m *FileManager) getFileUnsafe(filename string) (File, error) {
	f, ok := m.openFiles[filename]
	if ok {
		return f, nil
	}
	f, err := m.fs.Open(filename)
	if err != nil {
		return nil, err
	}
	m.openFiles[filename] = f
	return f, nil
}

func (m *FileManager) lookupFile(filename string) (File, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.isClosed {
		return nil, false
	}
	f, ok := m.openFiles[filename]
	return f, ok
}

func (m *FileManager) getFile(filename string) (File, error) {
	if f, ok := m.lookupFile(filename); ok {
		return f, nil
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.isClosed {
		return nil, ErrFileManagerClosed
	}
	return m.getFileUnsafe(filename)
}

func (m *FileManager) ReadBlock(b BlockID, p *Page) error {
	f, err := m.getFile(b.Filename)
	if err != nil {
		return fmt.Errorf("filemanager read block: get file %s: %w", b.Filename, err)
	}
	off := b.Number * m.blockSize
	_, err = f.ReadAt(p.Bytes(), off)
	if err != nil {
		return fmt.Errorf("filemanager read block: file %s, block %d, offset %d: %w", b.Filename, b.Number, off, err)
	}
	return nil
}

func (m *FileManager) WriteBlock(b BlockID, p *Page) error {
	f, err := m.getFile(b.Filename)
	if err != nil {
		return fmt.Errorf("filemanager write block: get file %s: %w", b.Filename, err)
	}
	off := b.Number * m.blockSize
	_, err = f.WriteAt(p.Bytes(), off)
	if err != nil {
		return fmt.Errorf("filemanager write block: get file %s: %w", b.Filename, err)
	}
	return nil
}

func (m *FileManager) AppendBlock(filename string) (BlockID, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.isClosed {
		return BlockID{}, ErrFileManagerClosed
	}
	f, err := m.getFileUnsafe(filename)
	if err != nil {
		return BlockID{}, fmt.Errorf("filemanager append block: size file %s: %w", filename, err)
	}
	size, err := f.Size()
	if err != nil {
		return BlockID{}, fmt.Errorf("filemanager append block: size file %s: %w", filename, err)
	}
	_, err = f.WriteAt(make([]byte, m.blockSize), size)
	if err != nil {
		return BlockID{}, fmt.Errorf("filemanager append block: write file %s: %w", filename, err)
	}
	return BlockID{
		Number:   size / m.blockSize,
		Filename: filename,
	}, nil
}

func (m *FileManager) BlockCount(filename string) (int64, error) {
	f, err := m.getFile(filename)
	if err != nil {
		return 0, fmt.Errorf("filemanager block count: get file %s: %w", filename, err)
	}
	size, err := f.Size()
	if err != nil {
		return 0, fmt.Errorf("filemanager block count: size file %s: %w", filename, err)
	}
	return size / m.blockSize, nil
}

func (m *FileManager) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.isClosed {
		return ErrFileManagerClosed
	}
	errs := make([]error, 0, len(m.openFiles))
	for _, f := range m.openFiles {
		err := f.Close()
		if err != nil {
			errs = append(errs, err)
		}
	}
	m.openFiles = nil
	m.isClosed = true
	return errors.Join(errs...)
}
