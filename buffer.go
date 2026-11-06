package scioredb

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"
)

type Buffer struct {
	fileMgr *FileManager
	logMgr  *LogManager
	page    *Page
	blk     BlockID
	pins    int
	txNum   int64
	lastLSN int64
	isDirty bool
	mu      sync.Mutex
}

func NewBuffer(fileMgr *FileManager, logMgr *LogManager, blockSize int64) *Buffer {
	return &Buffer{
		fileMgr: fileMgr,
		logMgr:  logMgr,
		page:    NewPage(blockSize),
		blk:     BlockID{Number: -1},
		txNum:   -1,
		lastLSN: -1,
		isDirty: false,
	}
}

func (b *Buffer) Page() *Page {
	return b.page
}

func (b *Buffer) Block() BlockID {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.blk
}

func (b *Buffer) TxID() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.txNum
}

func (b *Buffer) IsDirty() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.isDirty
}

func (b *Buffer) Pinned() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.pins > 0
}

func (b *Buffer) Pin() {
	b.mu.Lock()
	b.pins++
	b.mu.Unlock()
}

func (b *Buffer) Unpin() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.pins--
	if b.pins < 0 {
		panic("page buffer: pins count cannot be negative")
	}
}

func (b *Buffer) Flush() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.isDirty {
		return nil
	}

	if b.lastLSN >= 0 {
		if err := b.logMgr.Flush(b.lastLSN); err != nil {
			return err
		}
	}

	if err := b.fileMgr.WriteBlock(b.blk, b.page); err != nil {
		return err
	}

	b.isDirty = false
	return nil
}

func (b *Buffer) SetModified(txNum, lsn int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.isDirty = true
	b.txNum = txNum
	b.lastLSN = lsn
}

func (b *Buffer) AssignToBlock(blk BlockID) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.isDirty {
		if b.lastLSN >= 0 {
			if err := b.logMgr.Flush(b.lastLSN); err != nil {
				return fmt.Errorf("assign to block failed during WAL flush: %w", err)
			}
		}
		if err := b.fileMgr.WriteBlock(b.blk, b.page); err != nil {
			return fmt.Errorf("assign to block failed during old block write: %w", err)
		}
		b.isDirty = false
	}

	b.blk = blk
	b.txNum = -1
	b.lastLSN = -1

	if err := b.fileMgr.ReadBlock(blk, b.page); err != nil {
		b.blk = BlockID{Number: -1}
		return fmt.Errorf("critical: buffer assignment failed while reading disk block %s [#%d]: %w (buffer invalidated)",
			blk.Filename, blk.Number, err)
	}

	return nil
}

type BufferVictimStrategy func(pool []*Buffer) *Buffer

func NaiveVictimStrategy(pool []*Buffer) *Buffer {
	for _, pb := range pool {
		if !pb.Pinned() {
			return pb
		}
	}
	return nil
}

type BufferManagerOption func(p *BufferManager)

func WithTimeout(d time.Duration) BufferManagerOption {
	return func(p *BufferManager) {
		p.maxWaitTime = d
	}
}

func WithVictimStrategy(s BufferVictimStrategy) BufferManagerOption {
	return func(p *BufferManager) {
		p.victimStrategy = s
	}
}

func WithSpinInterval(d time.Duration) BufferManagerOption {
	return func(p *BufferManager) {
		p.spinInterval = d
	}
}

var ErrBufferTimeout = errors.New("buffer manager timeout: no available buffers in pool")

type BufferManager struct {
	pool           []*Buffer
	numAvailable   int
	victimStrategy BufferVictimStrategy
	maxWaitTime    time.Duration
	spinInterval   time.Duration
	mu             sync.Mutex
}

func NewBufferManager(fm *FileManager, lm *LogManager, numBuffers int, blockSize int64, opts ...BufferManagerOption) *BufferManager {
	m := &BufferManager{
		pool:           make([]*Buffer, numBuffers),
		numAvailable:   numBuffers,
		maxWaitTime:    10 * time.Second,
		spinInterval:   1 * time.Millisecond,
		victimStrategy: NaiveVictimStrategy,
	}

	for _, opt := range opts {
		opt(m)
	}

	for i := range numBuffers {
		m.pool[i] = NewBuffer(fm, lm, blockSize)
	}
	return m
}

func (m *BufferManager) findBuffer(blk BlockID) (*Buffer, bool) {
	for _, pb := range m.pool {
		if pb.Block() == blk {
			return pb, true
		}
	}
	return nil, false
}

func (m *BufferManager) Available() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.numAvailable
}

func (m *BufferManager) FlushAll() error {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, buf := range m.pool {
		if buf != nil && buf.IsDirty() {
			if err := buf.Flush(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (m *BufferManager) Pin(ctx context.Context, blk BlockID) (*Buffer, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	start := time.Now()

	for {
		if buf, found := m.findBuffer(blk); found {
			if !buf.Pinned() {
				m.numAvailable--
			}
			buf.Pin()
			return buf, nil
		}

		buf := m.victimStrategy(m.pool)
		if buf != nil {
			if !buf.Pinned() {
				m.numAvailable--
			}
			buf.Pin()

			m.mu.Unlock()
			err := buf.AssignToBlock(blk)
			m.mu.Lock()

			if err != nil {
				buf.Unpin()
				if !buf.Pinned() {
					m.numAvailable++
				}
				return nil, err
			}

			return buf, nil
		}

		elapsed := time.Since(start)
		remaining := m.maxWaitTime - elapsed

		if remaining <= 0 {
			return nil, ErrBufferTimeout
		}

		if err := ctx.Err(); err != nil {
			return nil, err
		}

		m.mu.Unlock()

		select {
		case <-ctx.Done():
			m.mu.Lock()
			return nil, ctx.Err()
		case <-time.After(m.spinInterval):
			m.mu.Lock()
		}
	}
}

func (m *BufferManager) Unpin(buf *Buffer) {
	m.mu.Lock()
	defer m.mu.Unlock()

	wasPinned := buf.Pinned()
	buf.Unpin()

	if wasPinned && !buf.Pinned() {
		m.numAvailable++
	}
}
