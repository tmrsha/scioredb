package scioredb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestBuffer(t *testing.T) {
	const (
		blockSize    = int64(100)
		logFilename  = "log.log"
		dataFilename = "data.dbf"
	)

	memFS := MemFS()

	fm := NewFileManager(memFS, blockSize)

	lm, err := NewLogManager(fm, logFilename, blockSize)
	if err != nil {
		t.Fatalf("NewLogManager failed: %v", err)
	}

	t.Run("PinningAndUnpinning", func(t *testing.T) {
		b := NewBuffer(fm, lm, blockSize)

		if b.Pinned() {
			t.Errorf("b.Pinned() = true; want false for newly created buffer")
		}

		b.Pin()
		if !b.Pinned() {
			t.Errorf("b.Pinned() = false; want true after Pin()")
		}

		b.Unpin()
		if b.Pinned() {
			t.Errorf("b.Pinned() = true; want false after Unpin()")
		}
	})

	t.Run("UnpinNegativePanicBarrier", func(t *testing.T) {
		b := NewBuffer(fm, lm, blockSize)

		defer func() {
			if r := recover(); r == nil {
				t.Errorf("b.Unpin() did not panic on negative pins count")
			}
		}()

		b.Unpin()
	})

	t.Run("AssignToBlock", func(t *testing.T) {
		b := NewBuffer(fm, lm, blockSize)

		blk0, err := fm.AppendBlock(dataFilename)
		if err != nil {
			t.Fatalf("fm.AppendBlock failed for blk0: %v", err)
		}

		blk1, err := fm.AppendBlock(dataFilename)
		if err != nil {
			t.Fatalf("fm.AppendBlock failed for blk1: %v", err)
		}

		if err := b.AssignToBlock(blk0); err != nil {
			t.Fatalf("pb.AssignToBlock(blk0) unexpected error: %v", err)
		}

		expectedVal := int32(42)
		if err := b.Page().PutInt32(20, expectedVal); err != nil {
			t.Fatalf("failed to modify underlying page: %v", err)
		}

		lsn, err := lm.AppendRecord([]byte("tx 1 modifies blk 0"))
		if err != nil {
			t.Fatalf("lm.AppendRecord failed: %v", err)
		}

		b.SetModified(1, lsn)

		if err := b.AssignToBlock(blk1); err != nil {
			t.Fatalf("b.AssignToBlock(blk1) unexpected error: %v", err)
		}

		file, err := memFS.Open(dataFilename)
		if err != nil {
			t.Fatalf("failed to open fake data file: %v", err)
		}

		diskPageBuf := make([]byte, blockSize)
		if _, err := file.ReadAt(diskPageBuf, 0); err != nil {
			t.Fatalf("failed to read block 0 from fake disk: %v", err)
		}

		diskPage := &Page{data: diskPageBuf}
		actualVal, err := diskPage.GetInt32(20)
		if err != nil {
			t.Fatalf("failed to parse int32 from disk data: %v", err)
		}

		if actualVal != expectedVal {
			t.Errorf("AssignToBlock() eviction failed\ngot from disk:  %d\nwant from disk: %d", actualVal, expectedVal)
		}

		if b.isDirty {
			t.Errorf("b.isDirty = true; want false after successful flush during eviction")
		}
	})
}

func TestBufferManager(t *testing.T) {
	const (
		blockSize    = int64(100)
		logFilename  = "log.log"
		dataFilename = "data.dbf"
	)
	memFS := MemFS()

	fm := NewFileManager(memFS, blockSize)

	lm, err := NewLogManager(fm, logFilename, blockSize)
	if err != nil {
		t.Fatalf("NewLogManager() failed: %v", err)
	}

	t.Run("PinAndUnpin", func(t *testing.T) {
		bm := NewBufferManager(fm, lm, 3, blockSize)
		ctx := context.Background()

		blk0, _ := fm.AppendBlock(dataFilename)
		blk1, _ := fm.AppendBlock(dataFilename)

		bp0, err := bm.Pin(ctx, blk0)
		if err != nil {
			t.Fatalf("bm.Pin(blk0) unexpected error: %v", err)
		}
		if bp0.Block() != blk0 {
			t.Errorf("got buffer for block %v, want %v", bp0.Block(), blk0)
		}

		bp0Retry, err := bm.Pin(ctx, blk0)
		if err != nil {
			t.Fatalf("pool.Pin(blk0) retry failed: %v", err)
		}
		if bp0Retry != bp0 {
			t.Errorf("bm.Pin() did not reuse cached buffer from memory")
		}

		if want := 2; bm.Available() != want {
			t.Errorf("bm.numAvailable = %d; want %d", bm.Available(), want)
		}

		bm.Unpin(bp0)
		bm.Unpin(bp0Retry)

		if want := 3; bm.numAvailable != want {
			t.Errorf("bm.numAvailable = %d after fully unpinning; want %d", bm.numAvailable, want)
		}

		bp1, err := bm.Pin(ctx, blk1)
		if err != nil {
			t.Fatalf("bm.Pin(blk1) failed: %v", err)
		}
		bm.Unpin(bp1)
	})

	t.Run("Concurrent", func(t *testing.T) {
		const (
			numBuffers = 2
			numWorkers = 10
		)

		bm := NewBufferManager(fm, lm, numBuffers, blockSize, WithSpinInterval(100*time.Microsecond))
		ctx := context.Background()

		blocks := make([]BlockID, numWorkers)
		for i := range numWorkers {
			blk, err := fm.AppendBlock(fmt.Sprintf("stress_%+v.tbl", i))
			if err != nil {
				t.Fatalf("fm.AppendBlock() failed for worker %d: %v", i, err)
			}
			blocks[i] = blk
		}

		var wg sync.WaitGroup
		wg.Add(numWorkers)

		for i := range numWorkers {
			go func(workerID int) {
				defer wg.Done()

				bp, err := bm.Pin(ctx, blocks[workerID])
				if err != nil {
					return
				}
				time.Sleep(1 * time.Millisecond)
				bm.Unpin(bp)
			}(i)
		}

		wg.Wait()

		if bm.Available() != numBuffers {
			t.Errorf("bm.numAvailable = %d after stress test; want %d (leak detected)", bm.numAvailable, numBuffers)
		}
	})

	t.Run("ContextTimeout", func(t *testing.T) {
		bm := NewBufferManager(fm, lm, 1, blockSize, WithTimeout(50*time.Millisecond), WithSpinInterval(50*time.Microsecond))
		ctx := context.Background()

		blk0, _ := fm.AppendBlock("timeout_test.tbl")
		blk1, _ := fm.AppendBlock("timeout_test.tbl")

		_, err = bm.Pin(ctx, blk0)
		if err != nil {
			t.Fatalf("first pin failed: %v", err)
		}

		_, err = bm.Pin(ctx, blk1)

		if err == nil {
			t.Errorf("bm.Pin() for blk1 succeeded; want ErrBufferTimeout when bm is fully locked")
		}
		if !errors.Is(ErrBufferTimeout, err) {
			t.Errorf("got error %v; want %v", err, ErrBufferTimeout)
		}
	})
}
