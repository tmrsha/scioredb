package scioredb

import (
	"bytes"
	"fmt"
	"testing"
)

func TestLogManager(t *testing.T) {
	const (
		blockSize = int64(100)
		logFile   = "test_wal.log"
	)
	fm := NewFileManager(MemFS(), blockSize)

	lm, err := NewLogManager(fm, logFile, blockSize)
	if err != nil {
		t.Fatalf("failed to create LogManager: %v", err)
	}

	const totalRecords = 20

	expectedRecords := make([][]byte, totalRecords)
	for i := 0; i < totalRecords; i++ {
		expectedRecords[i] = fmt.Appendf(nil, "record-%02d", i)
	}

	t.Run("AppendRecords", func(t *testing.T) {
		var lastLSN int64 = 0

		for i := 0; i < totalRecords; i++ {
			lsn, err := lm.AppendRecord(expectedRecords[i])
			if err != nil {
				t.Fatalf("failed to append record %d: %v", i, err)
			}

			if lsn <= lastLSN {
				t.Errorf("expected LSN to increase, got current=%d, last=%d", lsn, lastLSN)
			}
			lastLSN = lsn
		}

		if err := lm.Flush(lastLSN); err != nil {
			t.Fatalf("lm.Flush(%d) failed: %v", lastLSN, err)
		}
	})

	t.Run("VerifyReverseIteration", func(t *testing.T) {
		checkIndex := totalRecords - 1

		for record, err := range lm.Iterator() {
			if err != nil {
				t.Fatalf("iterator returned unexpected I/O error: %v", err)
			}

			expected := expectedRecords[checkIndex]

			if !bytes.Equal(record, expected) {
				t.Errorf("lm.Iterator() mismatch at index %d\ngot:  %q\nwant: %q", checkIndex, record, expected)
			}

			checkIndex--
		}

		if checkIndex != -1 {
			t.Errorf("iterator finished early, missed %d records", checkIndex+1)
		}
	})

	t.Run("VerifyGroupCommit", func(t *testing.T) {
		if err := lm.Flush(lm.lastSavedLSN); err != nil {
			t.Errorf("Flush should be no-op for saved LSN, got: %v", err)
		}
	})
}
