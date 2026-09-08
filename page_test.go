package scioredb

import (
	"errors"
	"testing"
)

func TestPage(t *testing.T) {

	t.Run("put and get valid data", func(t *testing.T) {
		p := NewPage(4096)
		wantInt := int32(199)
		wantStr := "hello world"

		if err := p.PutInt32(0, wantInt); err != nil {
			t.Fatalf("p.PutInt32(0, %d) failed: %v", wantInt, err)
		}
		if err := p.PutString(4, wantStr); err != nil {
			t.Fatalf("p.PutString(4, %q) failed: %v", wantStr, err)
		}

		gotInt, err := p.GetInt32(0)
		if err != nil {
			t.Fatalf("p.GetInt32(0) failed: %v", err)
		}
		gotStr, err := p.GetString(4)
		if err != nil {
			t.Fatalf("p.GetString(4) failed: %v", err)
		}

		if gotInt != wantInt {
			t.Errorf("p.GetInt32(0) = %d, want %d", gotInt, wantInt)
		}
		if gotStr != wantStr {
			t.Errorf("p.GetString(4) = %q, want %q", gotStr, wantStr)
		}
	})

	t.Run("boundary errors", func(t *testing.T) {
		p := NewPage(4096)

		if err := p.PutInt32(-1, 10); !errors.Is(err, ErrNegativeOffset) {
			t.Errorf("p.PutInt32(-1, 10) err = %v, want %v", err, ErrNegativeOffset)
		}

		if _, err := p.GetInt32(4096); !errors.Is(err, ErrIndexOutOfBounds) {
			t.Errorf("p.GetInt32(4096) err = %v, want %v", err, ErrIndexOutOfBounds)
		}

		largeBuf := make([]byte, 5000)
		if err := p.PutBytes(0, largeBuf); !errors.Is(err, ErrIndexOutOfBounds) {
			t.Errorf("p.PutBytes(0, largeBuf) err = %v, want %v", err, ErrIndexOutOfBounds)
		}
		if _, err := p.GetBytes(4095); !errors.Is(err, ErrIndexOutOfBounds) {
			t.Errorf("p.GetBytes(4095) err = %v, want %v", err, ErrIndexOutOfBounds)
		}
	})
}
