package scioredb

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

func TestLockTable(t *testing.T) {
	blk := BlockID{Filename: "data.db", Number: 0}

	t.Run("DefaultSpinInterval_And_SharedLocks", func(t *testing.T) {
		lt := NewLockTable(4, 0)
		ctx := context.Background()

		err1 := lt.SLock(ctx, blk)
		err2 := lt.SLock(ctx, blk)
		err3 := lt.SLock(ctx, blk)

		if err1 != nil || err2 != nil || err3 != nil {
			t.Fatalf("failed to acquire concurrent Shared Locks: %v, %v, %v", err1, err2, err3)
		}

		bucket := lt.getBucket(blk)
		if lt.lockValue(bucket, blk) != 3 {
			t.Errorf("unexpected lock value for 3 readers: got %d, want 3", lt.lockValue(bucket, blk))
		}

		lt.Unlock(blk)
		lt.Unlock(blk)
		lt.Unlock(blk)

		if lt.lockValue(bucket, blk) != 0 {
			t.Errorf("lock value should be 0 after all unlocks: got %d", lt.lockValue(bucket, blk))
		}
	})

	t.Run("ExclusiveLock_Blocks_ReadersAndWriters", func(t *testing.T) {
		lt := NewLockTable(4, 1*time.Millisecond)
		ctx := context.Background()

		if err := lt.XLock(ctx, blk); err != nil {
			t.Fatalf("failed to acquire X-Lock: %v", err)
		}

		bucket := lt.getBucket(blk)
		if !lt.hasXLock(bucket, blk) {
			t.Errorf("X-Lock flag not set correctly in bucket")
		}

		readCtx, readCancel := context.WithTimeout(ctx, 15*time.Millisecond)
		defer readCancel()

		errSLock := lt.SLock(readCtx, blk)
		if !errors.Is(errSLock, ErrLockTimeout) {
			t.Errorf("expected ErrLockTimeout for S-Lock, got: %v", errSLock)
		}

		writeCtx, writeCancel := context.WithTimeout(ctx, 15*time.Millisecond)
		defer writeCancel()

		errXLock := lt.XLock(writeCtx, blk)
		if !errors.Is(errXLock, ErrLockTimeout) {
			t.Errorf("expected ErrLockTimeout for X-Lock, got: %v", errXLock)
		}

		lt.Unlock(blk)
		if lt.lockValue(bucket, blk) != 0 {
			t.Errorf("lock value should be 0 after X-Lock unlock: got %d", lt.lockValue(bucket, blk))
		}
	})

	t.Run("Context_Cancellation_Aborts_Waiting", func(t *testing.T) {
		lt := NewLockTable(2, 5*time.Millisecond)
		ctx := context.Background()

		_ = lt.XLock(ctx, blk)

		cancelCtx, cancel := context.WithCancel(ctx)

		var wg sync.WaitGroup
		wg.Add(1)

		var subtestErr error
		go func() {
			defer wg.Done()
			subtestErr = lt.SLock(cancelCtx, blk)
		}()

		time.Sleep(10 * time.Millisecond)

		cancel()
		wg.Wait()

		if !errors.Is(subtestErr, ErrLockTimeout) {
			t.Errorf("expected ErrLockTimeout upon explicit context cancellation, got: %v", subtestErr)
		}

		lt.Unlock(blk)
	})
}
