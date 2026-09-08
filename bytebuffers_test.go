package scioredb

import (
	"bytes"
	"errors"
	"io"
	"testing"
)

func TestOffsetBufferTest(t *testing.T) {
	t.Run("write and read bytes", func(t *testing.T) {
		var (
			off       int64 = 4
			wantBytes       = []byte("12345")
		)
		b := NewOffsetBuffer(nil)
		inputFirst := []byte("5")
		n, err := b.WriteAt(inputFirst, off)
		if err != nil {
			t.Fatalf("b.WriteAt(..., %d) failed: %v", off, err)
		}
		if n != len(inputFirst) {
			t.Errorf("b.WriteAt(..., %d) = %d, want %d", off, n, len(inputFirst))
		}
		gotPrefix := b.Bytes()[:off]
		wantPrefix := make([]byte, off)
		if !bytes.Equal(gotPrefix, wantPrefix) {
			t.Errorf("accordion effect failed: got prefix %v, want %v", gotPrefix, wantPrefix)
		}
		inputSecond := []byte("1234")
		if _, err = b.WriteAt(inputSecond, 0); err != nil {
			t.Fatalf("b.WriteAt(..., 0) failed: %v", err)
		}
		p := make([]byte, len(wantBytes))
		if _, err = b.ReadAt(p, 0); err != nil {
			t.Fatalf("b.ReadAt(p, 0) failed: %v", err)
		}
		if !bytes.Equal(p, wantBytes) {
			t.Errorf("assembly failed: read slice = %q, want %q", p, wantBytes)
		}
	})

	t.Run("boundary cases and errors", func(t *testing.T) {
		b := NewOffsetBuffer(nil)
		if _, err := b.WriteAt([]byte("test"), -1); !errors.Is(err, ErrNegativeOffset) {
			t.Errorf("b.WriteAt with negative offset: got err = %v, want %v", err, ErrNegativeOffset)
		}
		p := make([]byte, 10)
		if _, err := b.ReadAt(p, -1); !errors.Is(err, ErrNegativeOffset) {
			t.Errorf("b.ReadAt with negative offset: got err = %v, want %v", err, ErrNegativeOffset)
		}
		if n, err := b.ReadAt(nil, 100); n != 0 || err != nil {
			t.Errorf("b.ReadAt with empty slice: got (n=%d, err=%v), want (0, nil)", n, err)
		}

		if n, err := b.ReadAt(p, 100); n != 0 || !errors.Is(err, io.EOF) {
			t.Errorf("b.ReadAt past EOF: got (n=%d, err=%v), want (0, %v)", n, err, io.EOF)
		}
		bWithData := NewOffsetBuffer([]byte{1, 2, 3})
		pLarge := make([]byte, 10)
		n, err := bWithData.ReadAt(pLarge, 0)
		if n != 3 || !errors.Is(err, io.EOF) {
			t.Errorf("b.ReadAt partial read: got (n=%d, err=%v), want (3, %v)", n, err, io.EOF)
		}
	})

}
