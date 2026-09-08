package scioredb

import (
	"encoding/binary"
)

type Page struct {
	data []byte
}

func NewPage(size int) *Page {
	return &Page{
		data: make([]byte, size),
	}
}

func (p *Page) PutInt32(off int64, i int32) error {
	if off < 0 {
		return ErrNegativeOffset
	}
	if off+4 > int64(len(p.data)) {
		return ErrIndexOutOfBounds
	}
	binary.LittleEndian.PutUint32(p.data[off:], uint32(i))
	return nil
}

func (p *Page) GetInt32(off int64) (int32, error) {
	if off < 0 {
		return 0, ErrNegativeOffset
	}
	if off+4 >= int64(len(p.data)) {
		return 0, ErrIndexOutOfBounds
	}
	i := binary.LittleEndian.Uint32(p.data[off : off+4])
	return int32(i), nil
}

func (p *Page) PutBytes(off int64, b []byte) error {
	if off < 0 {
		return ErrNegativeOffset
	}
	if off+int64(len(b))+4 > int64(len(p.data)) {
		return ErrIndexOutOfBounds
	}
	binary.LittleEndian.PutUint32(p.data[off:], uint32(len(b)))
	copy(p.data[off+4:], b)
	return nil
}

func (p *Page) GetBytes(off int64) ([]byte, error) {
	if off < 0 {
		return nil, ErrNegativeOffset
	}
	if off+4 >= int64(len(p.data)) {
		return nil, ErrIndexOutOfBounds
	}
	l := binary.LittleEndian.Uint32(p.data[off : off+4])
	start := off + 4
	end := start + int64(l)
	if end > int64(len(p.data)) {
		return nil, ErrIndexOutOfBounds
	}
	b := make([]byte, l)
	copy(b, p.data[start:off+end])
	return b, nil
}

// TODO(tmrsha): Avoid heap allocation from []byte(s). In Version 2, rewrite
// the method to copy the string directly using copy(p.data[off+4:], s)
// or use unsafe.Slice to eliminate performance overhead.
func (p *Page) PutString(off int64, s string) error {
	return p.PutBytes(off, []byte(s))
}

func (p *Page) GetString(off int64) (string, error) {
	b, err := p.GetBytes(off)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (p *Page) Bytes() []byte {
	return p.data
}
