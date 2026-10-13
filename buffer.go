package scioredb

import (
	"context"
	"sync"
	"time"
)

type Buffer struct {
	fm      *FileManager
	lm      *LogManager
	page    *Page
	blk     BlockID
	pins    int
	txNum   int64
	lastLSN int64
	isDirty bool
}

func NewBuffer(f *FileManager, l *LogManager, blockSize int64) *Buffer {
	return &Buffer{
		fm:      f,
		lm:      l,
		page:    NewPage(blockSize),
		blk:     BlockID{Number: -1},
		txNum:   -1,
		lastLSN: -1,
	}
}

func (b *Buffer) flush() error {
	if !b.isDirty {
		return nil
	}

	if b.lastLSN >= 0 {
		if err := b.lm.Flush(b.lastLSN); err != nil {
			return err
		}
	}

	if err := b.fm.WriteBlock(b.blk, b.page); err != nil {
		return err
	}

	b.isDirty = false
	return nil
}

func (b *Buffer) Page() *Page {
	return b.page
}

func (b *Buffer) Block() BlockID {
	return b.blk
}

func (b *Buffer) TxID() int64 {
	return b.txNum
}

func (b *Buffer) IsDirty() bool {
	return b.isDirty
}

func (b *Buffer) Pinned() bool {
	return b.pins > 0
}

func (b *Buffer) Pin() {
	b.pins++
}

func (b *Buffer) Unpin() {
	b.pins--
	if b.pins < 0 {
		panic("page buffer: pins count cannot be negative")
	}
}

func (b *Buffer) SetModified(txNum, lsn int64) {
	b.isDirty = true
	b.txNum = txNum
	if lsn >= 0 {
		b.lastLSN = lsn
	}
}

func (b *Buffer) AssignToBlock(blk BlockID) error {
	err := b.flush()
	if err != nil {
		return err
	}
	b.blk = blk
	err = b.fm.ReadBlock(blk, b.page)
	if err != nil {
		return err
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

func WithVictimStrategy(strategy BufferVictimStrategy) BufferManagerOption {
	return func(p *BufferManager) {
		p.victimStrategy = strategy
	}
}

func WithSpinInterval(d time.Duration) BufferManagerOption {
	return func(p *BufferManager) {
		p.spinInterval = d
	}
}

type BufferManager struct {
	pool           []*Buffer
	numAvailable   int
	mu             sync.Mutex
	victimStrategy BufferVictimStrategy
	maxWaitTime    time.Duration
	spinInterval   time.Duration
}

func NewBufferManager(fm *FileManager, lm *LogManager, numBuffers int, blockSize int64, opts ...BufferManagerOption) *BufferManager {
	m := &BufferManager{
		pool:           make([]*Buffer, numBuffers),
		numAvailable:   numBuffers,
		victimStrategy: NaiveVictimStrategy,
		maxWaitTime:    10 * time.Second,
		spinInterval:   1 * time.Millisecond,
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

func (m *BufferManager) tryPin(blk BlockID) (*Buffer, error) {
	pb, ok := m.findBuffer(blk)
	if !ok {
		pb = m.victimStrategy(m.pool)
		if pb == nil {
			return nil, nil
		}
		err := pb.AssignToBlock(blk)
		if err != nil {
			return nil, err
		}
	}
	if !pb.Pinned() {
		m.numAvailable--
	}
	pb.Pin()
	return pb, nil
}

func (m *BufferManager) Available() int {
	return m.numAvailable
}

func (m *BufferManager) Pin(ctx context.Context, blk BlockID) (*Buffer, error) {
	pinCtx, cancel := context.WithTimeout(ctx, m.maxWaitTime)
	defer cancel()
	for {
		if m.mu.TryLock() {
			pb, err := m.tryPin(blk)
			if err != nil {
				m.mu.Unlock()
				return nil, err
			}
			if pb != nil {
				m.mu.Unlock()
				return pb, nil
			}

			if err = pinCtx.Err(); err != nil {
				m.mu.Unlock()
				return nil, ErrBufferTimeout
			}
			m.mu.Unlock()
		}

		select {
		case <-pinCtx.Done():
			return nil, ErrBufferTimeout
		case <-time.After(m.spinInterval):
			continue
		}
	}
}

func (m *BufferManager) Unpin(buf *Buffer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	buf.Unpin()
	if !buf.Pinned() {
		m.numAvailable++
	}
}
