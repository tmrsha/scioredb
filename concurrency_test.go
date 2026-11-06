package scioredb

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestConcurrencyManager(t *testing.T) {
	blk := BlockID{Filename: "table.db", Number: 0}

	t.Run("Lock_Elision_And_Reentrancy", func(t *testing.T) {
		lt := NewLockTable(4, 50*time.Microsecond)
		cm := NewConcurrencyManager(lt)
		ctx := context.Background()

		if err := cm.SLock(ctx, blk); err != nil {
			t.Fatalf("initial SLock failed: %v", err)
		}

		if lock, ok := cm.locks[blk]; !ok || lock != SLockLock {
			t.Errorf("SLock marker missing or incorrect in local map")
		}

		if err := cm.SLock(ctx, blk); err != nil {
			t.Fatalf("re-entrant SLock failed: %v", err)
		}

		bucket := lt.getBucket(blk)
		if lt.lockValue(bucket, blk) != 1 {
			t.Errorf("Lock Table value should be 1 due to local cache hit, got: %d", lt.lockValue(bucket, blk))
		}

		cm.ReleaseAll()
		if lt.lockValue(bucket, blk) != 0 {
			t.Errorf("Lock Table was not cleared completely")
		}
	})

	t.Run("Lock_Upgrade_Shared_To_Exclusive", func(t *testing.T) {
		lt := NewLockTable(4, 50*time.Microsecond)
		cm := NewConcurrencyManager(lt)
		ctx := context.Background()

		_ = cm.SLock(ctx, blk)

		if err := cm.XLock(ctx, blk); err != nil {
			t.Fatalf("Lock Upgrade failed: %v", err)
		}

		if lock, ok := cm.locks[blk]; !ok || lock != XLockLock {
			t.Errorf("local lock type was not upgraded to XLockLock")
		}

		bucket := lt.getBucket(blk)
		if lt.lockValue(bucket, blk) != -1 {
			t.Errorf("Lock Table value should be -1, got: %d", lt.lockValue(bucket, blk))
		}

		cm.ReleaseAll()
	})

	t.Run("Inter_Transaction_Blocking_And_Release", func(t *testing.T) {
		lt := NewLockTable(4, 50*time.Microsecond)
		ctx := context.Background()

		cm1 := NewConcurrencyManager(lt)
		cm2 := NewConcurrencyManager(lt)

		if err := cm1.XLock(ctx, blk); err != nil {
			t.Fatalf("Tx1 failed to acquire XLock: %v", err)
		}

		var wg sync.WaitGroup
		wg.Add(1)

		var tx2Err error
		tx2ReadyToLock := make(chan struct{})
		tx2AcquiredLock := make(chan struct{})

		go func() {
			defer wg.Done()

			close(tx2ReadyToLock)

			tx2Err = cm2.SLock(ctx, blk)

			close(tx2AcquiredLock)
		}()

		<-tx2ReadyToLock

		for range 5 {
			time.Sleep(50 * time.Microsecond)
		}

		cm1.ReleaseAll()

		wg.Wait()

		if tx2Err != nil {
			t.Errorf("Tx2 failed to acquire lock after Tx1 released it: %v", tx2Err)
		}

		bucket := lt.getBucket(blk)
		if lt.lockValue(bucket, blk) != 1 {
			t.Errorf("Tx2 should hold a valid Shared Lock now, got value: %d", lt.lockValue(bucket, blk))
		}

		cm2.ReleaseAll()
	})
}
