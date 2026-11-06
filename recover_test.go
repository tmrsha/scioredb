package scioredb

import (
	"context"
	"encoding/binary"
	"testing"
)

func TestRecover(t *testing.T) {
	ctx := context.Background()
	const (
		testBlockSize = 4096
		filename      = "test.dbf"
		logFilename   = "log.log"
	)

	fs := MemFS()
	fm := NewFileManager(fs, testBlockSize)
	lm, _ := NewLogManager(fm, logFilename, testBlockSize)

	bm := NewBufferManager(fm, lm, 3, testBlockSize)

	blk, err := fm.AppendBlock(filename)
	if err != nil {
		t.Fatalf("failed to setup test block: %v", err)
	}

	const (
		offsetTx1 = int64(32)
		offsetTx2 = int64(64)
		baseVal1  = int32(10)
		baseVal2  = int32(20)
		newVal1   = int32(111)
		newVal2   = int32(222)
	)

	initBuf, err := bm.Pin(ctx, blk)
	if err != nil {
		t.Fatalf("failed to pin during initialization: %v", err)
	}
	_ = initBuf.Page().PutInt32(offsetTx1, baseVal1)
	_ = initBuf.Page().PutInt32(offsetTx2, baseVal2)
	initBuf.SetModified(0, 0)
	bm.Unpin(initBuf)
	if err := bm.FlushAll(); err != nil {
		t.Fatalf("failed to flush base values to disk: %v", err)
	}

	_, _ = lm.AppendRecord(LogRecord{Type: TxStart, TxID: 1})
	oldBytes1 := make([]byte, 4)
	binary.LittleEndian.PutUint32(oldBytes1, uint32(baseVal1))
	lsn1, _ := lm.AppendRecord(LogRecord{Type: TxSetBytes, TxID: 1, Block: blk, Offset: offsetTx1, RawValue: oldBytes1})

	bufTx1, _ := bm.Pin(ctx, blk)
	_ = bufTx1.Page().PutInt32(offsetTx1, newVal1)
	bufTx1.SetModified(1, lsn1)
	bm.Unpin(bufTx1)
	_, _ = lm.AppendRecord(LogRecord{Type: TxCommit, TxID: 1})

	_, _ = lm.AppendRecord(LogRecord{Type: TxStart, TxID: 2})
	oldBytes2 := make([]byte, 4)
	binary.LittleEndian.PutUint32(oldBytes2, uint32(baseVal2))
	lsn2, _ := lm.AppendRecord(LogRecord{Type: TxSetBytes, TxID: 2, Block: blk, Offset: offsetTx2, RawValue: oldBytes2})

	bufTx2, _ := bm.Pin(ctx, blk)
	_ = bufTx2.Page().PutInt32(offsetTx2, newVal2)
	bufTx2.SetModified(2, lsn2)
	bm.Unpin(bufTx2)

	if err := bm.FlushAll(); err != nil {
		t.Fatalf("failed to simulate steal policy via flush: %v", err)
	}

	bmAfterCrash := NewBufferManager(fm, lm, 3, testBlockSize)

	res, err := Recover(ctx, lm, bmAfterCrash)
	if err != nil {
		t.Fatalf("Recover() failed with unexpected error: %v", err)
	}

	verifyBuf, err := bmAfterCrash.Pin(ctx, blk)
	if err != nil {
		t.Fatalf("failed to pin verification block: %v", err)
	}
	defer bmAfterCrash.Unpin(verifyBuf)

	resVal1, _ := verifyBuf.Page().GetInt32(offsetTx1)
	if resVal1 != newVal1 {
		t.Errorf("Violation of Durability: Committed Tx 1 was rolled back! Got %d, Want %d", resVal1, newVal1)
	}

	resVal2, _ := verifyBuf.Page().GetInt32(offsetTx2)
	if resVal2 != baseVal2 {
		t.Errorf("Violation of Atomicity: Active Tx 2 failed to undo! Got %d, Want %d", resVal2, baseVal2)
	}

	if res.MaxTxID != 2 {
		t.Errorf("Violation of Space Invariant: RecoverResult.MaxTxID got %d, want 2 (active transactions must not be ignored)", res.MaxTxID)
	}
}
