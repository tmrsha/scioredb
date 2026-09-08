package scioredb

import (
	"fmt"
	"io"
)

type OffsetBuffer struct {
	buf []byte
}

func NewOffsetBuffer(buf []byte) *OffsetBuffer {
	return &OffsetBuffer{buf: buf}
}

func (b *OffsetBuffer) WriteAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, fmt.Errorf("OffsetBuffer.WriteAt: %w", ErrNegativeOffset)
	}
	minLen := off + int64(len(p))
	if minLen > int64(len(b.buf)) {
		buf := make([]byte, minLen)
		copy(buf, b.buf)
		b.buf = buf
	}
	n = copy(b.buf[off:], p)
	return
}

func (b *OffsetBuffer) ReadAt(p []byte, off int64) (n int, err error) {
	if off < 0 {
		return 0, fmt.Errorf("OffsetBuffer.ReadAt: %w", ErrNegativeOffset)
	}
	if len(p) == 0 {
		return 0, nil
	}
	if off >= int64(len(b.buf)) {
		return 0, io.EOF
	}
	available := int64(len(b.buf)) - off
	if available < int64(len(p)) {
		n = copy(p, b.buf[off:])
		return n, io.EOF
	}
	n = copy(p, b.buf[off:])
	return n, nil
}

// Bytes returns a slice of the underlying buffer.
// The returned slice references the same memory as the internal buffer,
// so modifications to the returned slice will affect the OffsetBuffer,
// and future writes to the OffsetBuffer may overwrite the returned data.
func (b *OffsetBuffer) Bytes() []byte {
	return b.buf
}
