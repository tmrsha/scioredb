package scioredb

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestRecoveryManager(t *testing.T) {
	ctx := context.Background()
	const testBlockSize = 4096

	t.Run("Commit", func(t *testing.T) {
		fs := MemFS()
		fm := NewFileManager(fs, testBlockSize)
		lm, err := NewLogManager(fm, "log.log", testBlockSize)
		if err != nil {
			t.Fatalf("failed to create log manager: %v", err)
		}
		bm := NewBufferManager(fm, lm, 3, testBlockSize)

		txID := int64(888)
		rm := NewRecoveryManager(ctx, lm, bm, txID)

		if err := rm.Commit(); err != nil {
			t.Fatalf("Commit() returned unexpected error: %v", err)
		}

		foundCommit := false
		for rec, err := range lm.Iterator() {
			if err != nil {
				t.Fatalf("failed to iterate WAL: %v", err)
			}
			if rec.TxID == txID && rec.Type == TxCommit {
				foundCommit = true
				break
			}
		}

		if !foundCommit {
			t.Errorf("Commit() failed to append TxCommit record to WAL for tx %d", txID)
		}
	})

	t.Run("Rollback", func(t *testing.T) {
		const smallBlockSize = 512
		const dataFile = "test.db"
		fs := MemFS()
		fm := NewFileManager(fs, smallBlockSize)
		lm, err := NewLogManager(fm, "log.log", smallBlockSize)
		if err != nil {
			t.Fatalf("failed to create log manager: %v", err)
		}
		bm := NewBufferManager(fm, lm, 3, smallBlockSize)

		blk, err := fm.AppendBlock(dataFile)
		if err != nil {
			t.Fatalf("failed to append test block: %v", err)
		}

		txID := int64(202)
		rm := NewRecoveryManager(ctx, lm, bm, txID)

		_, _ = lm.AppendRecord(LogRecord{Type: TxStart, TxID: txID})

		const offsetInt = int64(16)
		const offsetStr = int64(128)

		const originalInt = int32(77)
		const originalStr = "initial_stable_string"

		const dirtyInt = int32(99)
		const dirtyStr = "corrupted_very_long_string_that_completely_overwrites_page_memory"

		buf, err := bm.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("setup pin failed: %v", err)
		}
		_ = buf.Page().PutInt32(offsetInt, originalInt)
		_ = buf.Page().PutString(offsetStr, originalStr)
		buf.SetModified(0, 0)
		bm.Unpin(buf)
		_ = bm.FlushAll()

		buf, err = bm.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("pin failed: %v", err)
		}

		lsn1, err := rm.PrepareUpdate(buf, offsetInt, 4)
		if err != nil {
			t.Fatalf("PrepareUpdate failed: %v", err)
		}
		_ = buf.Page().PutInt32(offsetInt, dirtyInt)
		buf.SetModified(txID, lsn1)

		lsn2, err := rm.PrepareUpdateBytes(buf, offsetStr)
		if err != nil {
			t.Fatalf("PrepareUpdateBytes failed: %v", err)
		}
		_ = buf.Page().PutString(offsetStr, dirtyStr)
		buf.SetModified(txID, lsn2)

		bm.Unpin(buf)

		if err := bm.FlushAll(); err != nil {
			t.Fatalf("flush all failed: %v", err)
		}

		bm = NewBufferManager(fm, lm, 3, smallBlockSize)
		rm = NewRecoveryManager(ctx, lm, bm, txID)

		if err := rm.Rollback(); err != nil {
			t.Fatalf("Rollback() failed: %v", err)
		}

		verifyBuf, err := bm.Pin(ctx, blk)
		if err != nil {
			t.Fatalf("pin failed during verification: %v", err)
		}
		defer bm.Unpin(verifyBuf)

		actualInt, err := verifyBuf.Page().GetInt32(offsetInt)
		if err != nil {
			t.Fatalf("failed to read int32: %v", err)
		}
		if actualInt != originalInt {
			t.Errorf("Rollback failed for Int32! Got %d, Want %d", actualInt, originalInt)
		}

		actualStr, err := verifyBuf.Page().GetString(offsetStr)
		if err != nil {
			t.Fatalf("failed to read string: %v", err)
		}
		if actualStr != originalStr {
			t.Errorf("Rollback failed for String! Got %q, Want %q", actualStr, originalStr)
		}

		foundRollback := false
		for rec, err := range lm.Iterator() {
			if err != nil {
				t.Fatalf("failed to iterate WAL: %v", err)
			}
			if rec.TxID == txID && rec.Type == TxRollback {
				foundRollback = true
				break
			}
		}

		if !foundRollback {
			t.Errorf("Rollback() failed to write TxRollback marker to WAL")
		}
	})
}

func TestTX(t *testing.T) {
	ctx := context.Background()
	const (
		testBlockSize = 4096
		dbFile        = "test.dbf"
		logFile       = "log.log"
	)
	fs := MemFS()
	fm := NewFileManager(fs, testBlockSize)
	lm, _ := NewLogManager(fm, logFile, testBlockSize)
	bm := NewBufferManager(fm, lm, 4, testBlockSize)

	lt := NewLockTable(8, 50*time.Microsecond)
	idGen := &IDGenerator{}

	blk, err := fm.AppendBlock(dbFile)
	if err != nil {
		t.Fatalf("failed to allocate physical Block 0 on disk: %v", err)
	}

	t.Run("Commit_With_LockUpgrade_And_LocalCache", func(t *testing.T) {
		tx1, err := NewTX(ctx, idGen, lm, bm, lt)
		if err != nil {
			t.Fatalf("failed to spawn tx1: %v", err)
		}

		buf, err := tx1.Pin(blk)
		if err != nil {
			t.Fatalf("tx1 pin failed: %v", err)
		}

		buf2, err := tx1.Pin(blk)
		if err != nil {
			t.Fatalf("tx1 re-pin failed: %v", err)
		}
		if buf != buf2 {
			t.Errorf("local buffer list cache hit failed: pointers mismatch")
		}

		err = tx1.PutInt32(buf, 100, 999)
		if err != nil {
			t.Fatalf("tx1 lock upgrade or mutation failed: %v", err)
		}

		val, _ := buf.Page().GetInt32(100)
		if val != 999 {
			t.Errorf("unexpected value in pinned page: got %d, want 999", val)
		}

		if err := tx1.Commit(); err != nil {
			t.Fatalf("tx1 commit failed: %v", err)
		}
	})

	t.Run("Inter_Transaction_Isolation_And_Rollback", func(t *testing.T) {
		txWrite, _ := NewTX(ctx, idGen, lm, bm, lt)
		txRead, _ := NewTX(ctx, idGen, lm, bm, lt)

		bufW, err := txWrite.Pin(blk)
		if err != nil {
			t.Fatalf("txWrite initial pin failed: %v", err)
		}

		_ = txWrite.PutInt32(bufW, 200, 777)

		var wg sync.WaitGroup
		wg.Add(1)

		txReadReady := make(chan struct{})
		var txReadErr error

		go func() {
			defer wg.Done()
			close(txReadReady)

			_, txReadErr = txRead.Pin(blk)
		}()

		<-txReadReady

		for range 5 {
			time.Sleep(50 * time.Microsecond)
		}

		if err := txWrite.Rollback(); err != nil {
			t.Fatalf("txWrite rollback failed: %v", err)
		}

		wg.Wait()

		if txReadErr != nil {
			t.Errorf("txRead failed to scan block after txWrite rollback: %v", txReadErr)
		}

		bufR, _ := txRead.Pin(blk)
		finalVal, _ := bufR.Page().GetInt32(200)

		if finalVal == 777 {
			t.Errorf("Isolation violation: txRead saw uncommitted or rolled back data!")
		}

		_ = txRead.Commit()
	})
}
