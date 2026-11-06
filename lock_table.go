package scioredb

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"
)

var ErrLockTimeout = errors.New("lock timeout expired")

const defaultSpinInterval = 10 * time.Millisecond

type lockBucket struct {
	mu    sync.Mutex
	locks map[BlockID]int
}

type LockTable struct {
	buckets      []*lockBucket
	bucketMask   uint32
	spinInterval time.Duration
}

func NewLockTable(numBuckets int, spinInterval time.Duration) *LockTable {
	if numBuckets <= 0 || (numBuckets&(numBuckets-1)) != 0 {
		panic("numBuckets must be a power of 2")
	}

	resolvedInterval := spinInterval
	if resolvedInterval <= 0 {
		resolvedInterval = defaultSpinInterval
	}

	lt := &LockTable{
		buckets:      make([]*lockBucket, numBuckets),
		bucketMask:   uint32(numBuckets - 1),
		spinInterval: resolvedInterval,
	}

	for i := 0; i < numBuckets; i++ {
		lt.buckets[i] = &lockBucket{
			locks: make(map[BlockID]int),
		}
	}

	return lt
}

func (t *LockTable) SLock(ctx context.Context, b BlockID) error {
	bucket := t.getBucket(b)
	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	for t.hasXLock(bucket, b) {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%w: %v", ErrLockTimeout, err)
		}

		bucket.mu.Unlock()
		select {
		case <-ctx.Done():
			bucket.mu.Lock()
			return fmt.Errorf("%w: %v", ErrLockTimeout, ctx.Err())
		case <-time.After(t.spinInterval):
		}
		bucket.mu.Lock()
	}

	bucket.locks[b] = t.lockValue(bucket, b) + 1
	return nil
}

func (t *LockTable) XLock(ctx context.Context, b BlockID) error {
	bucket := t.getBucket(b)
	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	for t.lockValue(bucket, b) != 0 {
		if err := ctx.Err(); err != nil {
			return fmt.Errorf("%w: %v", ErrLockTimeout, err)
		}

		bucket.mu.Unlock()
		select {
		case <-ctx.Done():
			bucket.mu.Lock()
			return fmt.Errorf("%w: %v", ErrLockTimeout, ctx.Err())
		case <-time.After(t.spinInterval):
		}
		bucket.mu.Lock()
	}

	bucket.locks[b] = -1
	return nil
}

func (t *LockTable) Unlock(b BlockID) {
	bucket := t.getBucket(b)
	bucket.mu.Lock()
	defer bucket.mu.Unlock()

	v := t.lockValue(bucket, b)
	if v == 0 {
		return
	}

	if v > 1 {
		bucket.locks[b] = v - 1
	} else {
		delete(bucket.locks, b)
	}
}

func (t *LockTable) getBucket(b BlockID) *lockBucket {
	h := fnv.New32a()
	_, _ = h.Write([]byte(b.Filename))

	var blkBytes [8]byte
	binary.LittleEndian.PutUint64(blkBytes[:], uint64(b.Number))
	_, _ = h.Write(blkBytes[:])

	idx := h.Sum32() & t.bucketMask
	return t.buckets[idx]
}

func (t *LockTable) hasXLock(bucket *lockBucket, b BlockID) bool {
	return t.lockValue(bucket, b) < 0
}

func (t *LockTable) lockValue(bucket *lockBucket, b BlockID) int {
	if n, ok := bucket.locks[b]; ok {
		return n
	}
	return 0
}
