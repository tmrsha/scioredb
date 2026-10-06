package scioredb

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestPageBuffer(t *testing.T) {
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
		pb := NewPageBuffer(fm, lm, blockSize)

		if pb.Pinned() {
			t.Errorf("pb.Pinned() = true; want false for newly created buffer")
		}

		pb.Pin()
		if !pb.Pinned() {
			t.Errorf("pb.Pinned() = false; want true after Pin()")
		}

		pb.Unpin()
		if pb.Pinned() {
			t.Errorf("pb.Pinned() = true; want false after Unpin()")
		}
	})

	t.Run("Unpin_NegativePanicBarrier", func(t *testing.T) {
		pb := NewPageBuffer(fm, lm, blockSize)

		defer func() {
			if r := recover(); r == nil {
				t.Errorf("pb.Unpin() did not panic on negative pins count")
			}
		}()

		pb.Unpin()
	})

	t.Run("AssignToBlock", func(t *testing.T) {
		pb := NewPageBuffer(fm, lm, blockSize)

		blk0, err := fm.AppendBlock(dataFilename)
		if err != nil {
			t.Fatalf("fm.AppendBlock failed for blk0: %v", err)
		}

		blk1, err := fm.AppendBlock(dataFilename)
		if err != nil {
			t.Fatalf("fm.AppendBlock failed for blk1: %v", err)
		}

		if err := pb.AssignToBlock(blk0); err != nil {
			t.Fatalf("pb.AssignToBlock(blk0) unexpected error: %v", err)
		}

		expectedVal := int32(42)
		if err := pb.Page().PutInt32(20, expectedVal); err != nil {
			t.Fatalf("failed to modify underlying page: %v", err)
		}

		lsn, err := lm.AppendRecord([]byte("tx 1 modifies blk 0"))
		if err != nil {
			t.Fatalf("lm.AppendRecord failed: %v", err)
		}

		pb.SetModified(1, lsn)

		if err := pb.AssignToBlock(blk1); err != nil {
			t.Fatalf("pb.AssignToBlock(blk1) unexpected error: %v", err)
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

		if pb.isDirty {
			t.Errorf("pb.isDirty = true; want false after successful flush during eviction")
		}
	})
}

func TestPageBufferPool_Lifecycle(t *testing.T) {
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
		pool := NewPageBufferPool(fm, lm, 3, blockSize)
		ctx := context.Background()

		blk0, _ := fm.AppendBlock(dataFilename)
		blk1, _ := fm.AppendBlock(dataFilename)

		bp0, err := pool.Pin(ctx, blk0)
		if err != nil {
			t.Fatalf("pool.Pin(blk0) unexpected error: %v", err)
		}
		if bp0.Block() != blk0 {
			t.Errorf("got buffer for block %v, want %v", bp0.Block(), blk0)
		}

		bp0Retry, err := pool.Pin(ctx, blk0)
		if err != nil {
			t.Fatalf("pool.Pin(blk0) retry failed: %v", err)
		}
		if bp0Retry != bp0 {
			t.Errorf("pool.Pin() did not reuse cached buffer from memory")
		}

		if want := 2; pool.Available() != want {
			t.Errorf("pool.numAvailable = %d; want %d", pool.Available(), want)
		}

		pool.Unpin(bp0)
		pool.Unpin(bp0Retry)

		if want := 3; pool.numAvailable != want {
			t.Errorf("pool.numAvailable = %d after fully unpinning; want %d", pool.numAvailable, want)
		}

		bp1, err := pool.Pin(ctx, blk1)
		if err != nil {
			t.Fatalf("pool.Pin(blk1) failed: %v", err)
		}
		pool.Unpin(bp1)
	})

	t.Run("Concurrent", func(t *testing.T) {
		const (
			numBuffers = 2
			numWorkers = 10
		)

		pool := NewPageBufferPool(fm, lm, numBuffers, blockSize, WithSpinInterval(100*time.Microsecond))
		ctx := context.Background()

		blocks := make([]BlockID, numWorkers)
		for i := 0; i < numWorkers; i++ {
			blk, err := fm.AppendBlock(fmt.Sprintf("stress_%+v.tbl", i))
			if err != nil {
				t.Fatalf("fm.AppendBlock() failed for worker %d: %v", i, err)
			}
			blocks[i] = blk
		}

		var wg sync.WaitGroup
		wg.Add(numWorkers)

		for i := 0; i < numWorkers; i++ {
			go func(workerID int) {
				defer wg.Done()

				bp, err := pool.Pin(ctx, blocks[workerID])
				if err != nil {
					return
				}
				time.Sleep(1 * time.Millisecond)
				pool.Unpin(bp)
			}(i)
		}

		wg.Wait()

		if pool.Available() != numBuffers {
			t.Errorf("pool.numAvailable = %d after stress test; want %d (leak detected)", pool.numAvailable, numBuffers)
		}
	})

	t.Run("ContextTimeout", func(t *testing.T) {
		pool := NewPageBufferPool(fm, lm, 1, blockSize, WithTimeout(50*time.Millisecond), WithSpinInterval(50*time.Microsecond))
		ctx := context.Background()

		blk0, _ := fm.AppendBlock("timeout_test.tbl")
		blk1, _ := fm.AppendBlock("timeout_test.tbl")

		_, err = pool.Pin(ctx, blk0)
		if err != nil {
			t.Fatalf("first pin failed: %v", err)
		}

		_, err = pool.Pin(ctx, blk1)

		if err == nil {
			t.Errorf("pool.Pin() for blk1 succeeded; want ErrBufferTimeout when pool is fully locked")
		}
		if err != ErrBufferTimeout {
			t.Errorf("got error %v; want %v", err, ErrBufferTimeout)
		}
	})
}
