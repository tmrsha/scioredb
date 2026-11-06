package scioredb

import (
	"context"
	"errors"
	"fmt"
)

var (
	ErrInvalidRecordType = errors.New("invalid log record type")
	ErrMalformedRecord   = errors.New("log record is corrupted or truncated")
)

type RecoveryManager struct {
	logMgr *LogManager
	bufMgr *BufferManager
	txID   int64
	ctx    context.Context
}

func NewRecoveryManager(c context.Context, logMgr *LogManager, bufMgr *BufferManager, txID int64) *RecoveryManager {
	return &RecoveryManager{ctx: c, logMgr: logMgr, bufMgr: bufMgr, txID: txID}
}

func (m *RecoveryManager) Commit() error {
	r := LogRecord{
		Type: TxCommit,
		TxID: m.txID,
	}
	lsn, err := m.logMgr.AppendRecord(r)
	if err != nil {
		return err
	}
	return m.logMgr.Flush(lsn)
}

func (m *RecoveryManager) Rollback() error {
	for rec, err := range m.logMgr.Iterator() {
		if err != nil {
			return fmt.Errorf("rollback failed during log iteration: %w", err)
		}

		if rec.TxID != m.txID {
			continue
		}

		switch rec.Type {
		case TxStart:
			break
		case TxSetBytes:
			err = func() error {
				buf, err := m.bufMgr.Pin(m.ctx, rec.Block)
				if err != nil {
					return fmt.Errorf("failed to pin block %v: %w", rec.Block, err)
				}
				defer m.bufMgr.Unpin(buf)

				pageBytes := buf.Page().Bytes()

				if rec.Offset < 0 || rec.Offset+int64(len(rec.RawValue)) > int64(len(pageBytes)) {
					return fmt.Errorf("offset out of page bounds")
				}

				copy(pageBytes[rec.Offset:], rec.RawValue)

				buf.SetModified(m.txID, -1)
				return nil
			}()

			if err != nil {
				return fmt.Errorf("rollback failed to undo record: %w", err)
			}
		}

		if rec.Type == TxStart {
			break
		}
	}

	rollbackRec := LogRecord{
		Type: TxRollback,
		TxID: m.txID,
	}
	lsn, err := m.logMgr.AppendRecord(rollbackRec)
	if err != nil {
		return fmt.Errorf("failed to append TxRollback record: %w", err)
	}

	if err := m.logMgr.Flush(lsn); err != nil {
		return fmt.Errorf("failed to flush log after rollback: %w", err)
	}

	return nil
}

func (m *RecoveryManager) PrepareUpdate(buf *Buffer, offset int64, length int64) (int64, error) {
	pageBytes := buf.Page().Bytes()

	if offset < 0 || offset+length > int64(len(pageBytes)) {
		return 0, fmt.Errorf("prepare update out of bounds")
	}

	oldBytes := make([]byte, length)
	copy(oldBytes, pageBytes[offset:offset+length])

	r := LogRecord{
		Type:     TxSetBytes,
		TxID:     m.txID,
		Block:    buf.Block(),
		Offset:   offset,
		RawValue: oldBytes,
	}

	return m.logMgr.AppendRecord(r)
}

func (m *RecoveryManager) PrepareUpdateBytes(buf *Buffer, offset int64) (int64, error) {
	entirePage := buf.Page().Bytes()

	if offset < 0 || offset > int64(len(entirePage)) {
		return 0, fmt.Errorf("prepare update bytes: offset out of bounds")
	}

	totalLength := int64(len(entirePage)) - offset

	return m.PrepareUpdate(buf, offset, totalLength)
}

type PinnedBuffer struct {
	buf  *Buffer
	pins int
}

type TX struct {
	txID         int64
	recoveryMgr  *RecoveryManager
	concurrencyM *ConcurrencyManager
	bufMgr       *BufferManager
	ctx          context.Context
	buffers      map[BlockID]*PinnedBuffer
}

func NewTX(ctx context.Context, idGen *IDGenerator, logMgr *LogManager, bufMgr *BufferManager, lockTable *LockTable) (*TX, error) {
	txID := idGen.NextID()
	_, err := logMgr.AppendRecord(LogRecord{
		Type: TxStart,
		TxID: txID,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to spawn transaction context (WAL error on TxStart): %w", err)
	}
	return &TX{
		txID:         txID,
		recoveryMgr:  NewRecoveryManager(ctx, logMgr, bufMgr, txID),
		concurrencyM: NewConcurrencyManager(lockTable),
		bufMgr:       bufMgr,
		ctx:          ctx,
		buffers:      make(map[BlockID]*PinnedBuffer),
	}, nil
}

func (tx *TX) unpinAllBuffers() {
	for blk, pBuf := range tx.buffers {
		for i := 0; i < pBuf.pins; i++ {
			tx.bufMgr.Unpin(pBuf.buf)
		}
		delete(tx.buffers, blk)
	}
}

func (tx *TX) ID() int64 {
	return tx.txID
}

func (tx *TX) Pin(blk BlockID) (*Buffer, error) {
	if err := tx.concurrencyM.SLock(tx.ctx, blk); err != nil {
		return nil, fmt.Errorf("tx %d concurrency violation on block %v: %w", tx.txID, blk, err)
	}

	if pBuf, exists := tx.buffers[blk]; exists {
		return pBuf.buf, nil // ИСПРАВЛЕНИЕ: убран pBuf.pins++
	}

	buf, err := tx.bufMgr.Pin(tx.ctx, blk)
	if err != nil {
		return nil, fmt.Errorf("tx %d failed to pin buffer: %w", tx.txID, err)
	}

	tx.buffers[blk] = &PinnedBuffer{
		buf:  buf,
		pins: 1,
	}

	return buf, nil
}

func (tx *TX) Unpin(blk BlockID) error {
	pBuf, exists := tx.buffers[blk]
	if !exists || pBuf.pins <= 0 {
		return fmt.Errorf("tx %d protocol violation: buffer %v is not pinned by this transaction", tx.txID, blk)
	}

	pBuf.pins--
	if pBuf.pins == 0 {
		tx.bufMgr.Unpin(pBuf.buf)
		delete(tx.buffers, blk)
	}

	return nil
}

func (tx *TX) Commit() error {
	if err := tx.recoveryMgr.Commit(); err != nil {
		return fmt.Errorf("tx %d commit crashed during flush: %w", tx.txID, err)
	}

	tx.unpinAllBuffers()

	tx.concurrencyM.ReleaseAll()

	return nil
}

func (tx *TX) Rollback() error {
	if err := tx.recoveryMgr.Rollback(); err != nil {
		return fmt.Errorf("tx %d rollback crashed during undo: %w", tx.txID, err)
	}

	tx.unpinAllBuffers()

	tx.concurrencyM.ReleaseAll()

	return nil
}

func (tx *TX) PutInt32(buf *Buffer, offset int64, val int32) error {
	blk := buf.Block()

	if err := tx.concurrencyM.XLock(tx.ctx, blk); err != nil {
		return fmt.Errorf("tx %d write block denied on %v: %w", tx.txID, blk, err)
	}

	lsn, err := tx.recoveryMgr.PrepareUpdate(buf, offset, 4)
	if err != nil {
		return fmt.Errorf("tx %d WAL failure: %w", tx.txID, err)
	}

	if err := buf.Page().PutInt32(offset, val); err != nil {
		return fmt.Errorf("tx %d byte array overwrite failure: %w", tx.txID, err)
	}

	buf.SetModified(tx.txID, lsn)
	return nil
}

func (tx *TX) PutString(buf *Buffer, offset int64, val string) error {
	blk := buf.Block()

	if err := tx.concurrencyM.XLock(tx.ctx, blk); err != nil {
		return fmt.Errorf("tx %d write block denied on %v: %w", tx.txID, blk, err)
	}

	lsn, err := tx.recoveryMgr.PrepareUpdateBytes(buf, offset)
	if err != nil {
		return fmt.Errorf("tx %d WAL failure: %w", tx.txID, err)
	}

	if err := buf.Page().PutString(offset, val); err != nil {
		return fmt.Errorf("tx %d byte array overwrite failure: %w", tx.txID, err)
	}

	buf.SetModified(tx.txID, lsn)
	return nil
}
