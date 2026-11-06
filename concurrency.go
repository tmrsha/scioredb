package scioredb

import (
	"context"
	"fmt"
)

type LockType int

const (
	SLockLock LockType = iota
	XLockLock
)

type ConcurrencyManager struct {
	lockTable *LockTable
	locks     map[BlockID]LockType
}

func NewConcurrencyManager(lockTable *LockTable) *ConcurrencyManager {
	return &ConcurrencyManager{
		lockTable: lockTable,
		locks:     make(map[BlockID]LockType),
	}
}

func (cm *ConcurrencyManager) SLock(ctx context.Context, blk BlockID) error {
	if _, holds := cm.locks[blk]; holds {
		return nil
	}

	if err := cm.lockTable.SLock(ctx, blk); err != nil {
		return fmt.Errorf("failed to acquire Shared Lock: %w", err)
	}

	cm.locks[blk] = SLockLock
	return nil
}

func (cm *ConcurrencyManager) XLock(ctx context.Context, blk BlockID) error {
	if lock, holds := cm.locks[blk]; holds && lock == XLockLock {
		return nil
	}

	if lock, holds := cm.locks[blk]; holds && lock == SLockLock {
		cm.lockTable.Unlock(blk)
	}

	if err := cm.lockTable.XLock(ctx, blk); err != nil {
		if lock, holds := cm.locks[blk]; holds && lock == SLockLock {
			_ = cm.lockTable.SLock(ctx, blk)
		}
		return fmt.Errorf("failed to acquire Exclusive Lock (Lock Upgrade): %w", err)
	}

	// Успешно обновили статус до монопольного
	cm.locks[blk] = XLockLock
	return nil
}

func (cm *ConcurrencyManager) ReleaseAll() {
	for blk := range cm.locks {
		cm.lockTable.Unlock(blk)
		delete(cm.locks, blk)
	}
}
