package scioredb

import (
	"context"
	"sync"
	"time"
)

type PageBuffer struct {
	fm      *FileManager
	lm      *LogManager
	page    *Page
	blk     BlockID
	pins    int
	txNum   int64
	lastLSN int64
	isDirty bool
}

func NewPageBuffer(f *FileManager, l *LogManager, blockSize int64) *PageBuffer {
	return &PageBuffer{
		fm:      f,
		lm:      l,
		page:    NewPage(blockSize),
		blk:     BlockID{Number: -1},
		txNum:   -1,
		lastLSN: -1,
	}
}

func (b *PageBuffer) flush() error {
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

func (b *PageBuffer) Page() *Page {
	return b.page
}

func (b *PageBuffer) Block() BlockID {
	return b.blk
}

func (b *PageBuffer) TxID() int64 {
	return b.txNum
}

func (b *PageBuffer) IsDirty() bool {
	return b.isDirty
}

func (b *PageBuffer) Pinned() bool {
	return b.pins > 0
}

func (b *PageBuffer) Pin() {
	b.pins++
}

func (b *PageBuffer) Unpin() {
	b.pins--
	if b.pins < 0 {
		panic("page buffer: pins count cannot be negative")
	}
}

func (b *PageBuffer) SetModified(txNum, lsn int64) {
	b.isDirty = true
	b.txNum = txNum
	if lsn >= 0 {
		b.lastLSN = lsn
	}
}

func (b *PageBuffer) AssignToBlock(blk BlockID) error {
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

type BufferVictimStrategy func(pool []*PageBuffer) *PageBuffer

func NaiveVictimStrategy(pool []*PageBuffer) *PageBuffer {
	for _, pb := range pool {
		if !pb.Pinned() {
			return pb
		}
	}
	return nil
}

type BufferPoolOption func(p *PageBufferPool)

func WithTimeout(d time.Duration) BufferPoolOption {
	return func(p *PageBufferPool) {
		p.maxWaitTime = d
	}
}

func WithVictimStrategy(strategy BufferVictimStrategy) BufferPoolOption {
	return func(p *PageBufferPool) {
		p.victimStrategy = strategy
	}
}

func WithSpinInterval(d time.Duration) BufferPoolOption {
	return func(p *PageBufferPool) {
		p.spinInterval = d
	}
}

type PageBufferPool struct {
	pool           []*PageBuffer
	numAvailable   int
	mu             sync.Mutex
	victimStrategy BufferVictimStrategy
	maxWaitTime    time.Duration
	spinInterval   time.Duration
}

func NewPageBufferPool(fm *FileManager, lm *LogManager, numBuffers int, blockSize int64, opts ...BufferPoolOption) *PageBufferPool {
	p := &PageBufferPool{
		pool:           make([]*PageBuffer, numBuffers),
		numAvailable:   numBuffers,
		victimStrategy: NaiveVictimStrategy,
		maxWaitTime:    10 * time.Second,
		spinInterval:   1 * time.Millisecond,
	}

	for _, opt := range opts {
		opt(p)
	}

	for i := 0; i < numBuffers; i++ {
		p.pool[i] = NewPageBuffer(fm, lm, blockSize)
	}
	return p
}

func (p *PageBufferPool) findBuffer(blk BlockID) (*PageBuffer, bool) {
	for _, pb := range p.pool {
		if pb.Block() == blk {
			return pb, true
		}
	}
	return nil, false
}

func (p *PageBufferPool) tryPin(blk BlockID) (*PageBuffer, error) {
	pb, ok := p.findBuffer(blk)
	if !ok {
		pb = p.victimStrategy(p.pool)
		if pb == nil {
			return nil, nil
		}
		err := pb.AssignToBlock(blk)
		if err != nil {
			return nil, err
		}
	}
	if !pb.Pinned() {
		p.numAvailable--
	}
	pb.Pin()
	return pb, nil
}

func (p *PageBufferPool) Available() int {
	return p.numAvailable
}

func (p *PageBufferPool) Pin(ctx context.Context, blk BlockID) (*PageBuffer, error) {
	pinCtx, cancel := context.WithTimeout(ctx, p.maxWaitTime)
	defer cancel()
	for {
		if p.mu.TryLock() {
			pb, err := p.tryPin(blk)
			if err != nil {
				p.mu.Unlock()
				return nil, err
			}
			if pb != nil {
				p.mu.Unlock()
				return pb, nil
			}

			if err := pinCtx.Err(); err != nil {
				p.mu.Unlock()
				return nil, ErrBufferTimeout
			}
			p.mu.Unlock()
		}

		select {
		case <-pinCtx.Done():
			return nil, ErrBufferTimeout
		case <-time.After(p.spinInterval):
			continue
		}
	}
}

func (p *PageBufferPool) Unpin(buf *PageBuffer) {
	p.mu.Lock()
	defer p.mu.Unlock()
	buf.Unpin()
	if !buf.Pinned() {
		p.numAvailable++
	}
}
