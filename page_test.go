package scioredb

import (
	"errors"
	"testing"
)

func TestPage(t *testing.T) {
	const pageSize = int64(400)

	t.Run("HappyPath_Int32", func(t *testing.T) {
		p := NewPage(pageSize)
		off := int64(0)
		expected := int32(42)

		if err := p.PutInt32(off, expected); err != nil {
			t.Fatalf("unexpected error on PutInt32: %v", err)
		}

		actual, err := p.GetInt32(off)
		if err != nil {
			t.Fatalf("unexpected error on GetInt32: %v", err)
		}

		if actual != expected {
			t.Errorf("expected %d, got %d", expected, actual)
		}
	})

	t.Run("Int64", func(t *testing.T) {
		p := NewPage(pageSize)
		off := int64(10)
		expected := int64(12345678901234)

		if err := p.PutInt64(off, expected); err != nil {
			t.Fatalf("unexpected error on PutInt64: %v", err)
		}

		actual, err := p.GetInt64(off)
		if err != nil {
			t.Fatalf("unexpected error on GetInt64: %v", err)
		}

		if actual != expected {
			t.Errorf("expected %d, got %d", expected, actual)
		}
	})

	t.Run("BytesAndString", func(t *testing.T) {
		p := NewPage(pageSize)
		off := int64(20)
		expectedStr := "hello, google go style guide!"

		if err := p.PutString(off, expectedStr); err != nil {
			t.Fatalf("unexpected error on PutString: %v", err)
		}

		actualStr, err := p.GetString(off)
		if err != nil {
			t.Fatalf("unexpected error on GetString: %v", err)
		}

		if actualStr != expectedStr {
			t.Errorf("expected %q, got %q", expectedStr, actualStr)
		}
	})

	t.Run("InaccessibleEnd", func(t *testing.T) {
		p := NewPage(pageSize)

		off32 := pageSize - int32Size
		if err := p.PutInt32(off32, 99); err != nil {
			t.Errorf("failed to write int32 at the very end of the page: %v", err)
		}

		off64 := pageSize - int64Size
		if err := p.PutInt64(off64, 999); err != nil {
			t.Errorf("failed to write int64 at the very end of the page: %v", err)
		}
	})

	t.Run("NegativeOffset", func(t *testing.T) {
		p := NewPage(pageSize)
		negOff := int64(-5)

		if err := p.PutInt32(negOff, 1); !errors.Is(err, ErrNegativeOffset) {
			t.Errorf("expected %v, got %v", ErrNegativeOffset, err)
		}

		if _, err := p.GetInt64(negOff); !errors.Is(err, ErrNegativeOffset) {
			t.Errorf("expected %v, got %v", ErrNegativeOffset, err)
		}

		if err := p.PutBytes(negOff, []byte{1, 2}); !errors.Is(err, ErrNegativeOffset) {
			t.Errorf("expected %v, got %v", ErrNegativeOffset, err)
		}
	})

	t.Run("GuardClauses_IndexOutOfBounds", func(t *testing.T) {
		p := NewPage(pageSize)

		badOff32 := pageSize - int32Size + 2
		if err := p.PutInt32(badOff32, 1); !errors.Is(err, ErrIndexOutOfBounds) {
			t.Errorf("expected %v for int32 out of bounds, got %v", ErrIndexOutOfBounds, err)
		}

		badOff64 := pageSize - int64Size + 3
		if err := p.PutInt64(badOff64, 1); !errors.Is(err, ErrIndexOutOfBounds) {
			t.Errorf("expected %v for int64 out of bounds, got %v", ErrIndexOutOfBounds, err)
		}

		if _, err := p.GetInt32(badOff32); !errors.Is(err, ErrIndexOutOfBounds) {
			t.Errorf("expected %v on GetInt32 out of bounds, got %v", ErrIndexOutOfBounds, err)
		}

		strOff := pageSize - 10
		if err := p.PutString(strOff, "too long string"); !errors.Is(err, ErrIndexOutOfBounds) {
			t.Errorf("expected %v for oversized string, got %v", ErrIndexOutOfBounds, err)
		}
	})
}
