package scioredb

import (
	"context"
	"fmt"
)

type RecoverResult struct {
	MaxTxID int64
}

func Recover(c context.Context, logMgr *LogManager, bufMgr *BufferManager) (RecoverResult, error) {
	var (
		result      RecoverResult
		finishedTxs = make(map[int64]struct{})
		activeTxs   = make(map[int64]struct{})
	)

	for rec, err := range logMgr.Iterator() {
		if err != nil {
			return result, fmt.Errorf("recovery failed during log iteration: %w", err)
		}

		if rec.TxID > result.MaxTxID {
			result.MaxTxID = rec.TxID
		}

		switch rec.Type {
		case TxCommit, TxRollback:
			finishedTxs[rec.TxID] = struct{}{}

		case TxStart:
			if _, ok := activeTxs[rec.TxID]; ok {
				delete(activeTxs, rec.TxID)
			}

		case TxSetBytes:
			if _, ok := finishedTxs[rec.TxID]; ok {
				continue
			}

			activeTxs[rec.TxID] = struct{}{}

			err = func() error {
				buf, err := bufMgr.Pin(c, rec.Block)
				if err != nil {
					return fmt.Errorf("failed to pin block %v: %w", rec.Block, err)
				}
				defer bufMgr.Unpin(buf)

				pageBytes := buf.Page().Bytes()

				if rec.Offset < 0 || rec.Offset+int64(len(rec.RawValue)) > int64(len(pageBytes)) {
					return fmt.Errorf("recovery failed: log record offset out of page bounds")
				}

				copy(pageBytes[rec.Offset:], rec.RawValue)

				buf.SetModified(rec.TxID, -1)
				return nil
			}()

			if err != nil {
				return result, fmt.Errorf("recovery failed during undo operation: %w", err)
			}
		}
	}

	if len(activeTxs) > 0 {
		return result, fmt.Errorf("recovery failed: log integrity violation, orphan transactions detected (missing TxStart): %v", activeTxs)
	}

	if err := bufMgr.FlushAll(); err != nil {
		return result, fmt.Errorf("failed to flush buffers after recovery: %w", err)
	}

	return result, nil
}
