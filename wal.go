package scioredb

import (
	"encoding/binary"
	"fmt"
	"iter"
	"sync"
)

type RecordType int32

const (
	TxStart RecordType = iota
	TxCommit
	TxRollback
	TxSetBytes
)

func (t RecordType) String() string {
	switch t {
	case TxStart:
		return "START"
	case TxCommit:
		return "COMMIT"
	case TxRollback:
		return "ROLLBACK"
	case TxSetBytes:
		return "SET_BYTES"
	default:
		return "UNKNOWN"
	}
}

type LogRecord struct {
	Type     RecordType
	TxID     int64
	Block    BlockID
	Offset   int64
	RawValue []byte
}

func (r *LogRecord) MarshalBinary() ([]byte, error) {
	size := 4 + 8

	if r.Type == TxSetBytes {
		size += 4 + len(r.Block.Filename)
		size += 8
		size += 8
		size += 4 + len(r.RawValue)
	}

	buf := make([]byte, size)
	pos := 0

	binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(r.Type))
	pos += 4
	binary.LittleEndian.PutUint64(buf[pos:pos+8], uint64(r.TxID))
	pos += 8

	if r.Type == TxSetBytes {
		fnLen := len(r.Block.Filename)
		binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(fnLen))
		pos += 4
		copy(buf[pos:pos+fnLen], r.Block.Filename)
		pos += fnLen

		binary.LittleEndian.PutUint64(buf[pos:pos+8], uint64(r.Block.Number))
		pos += 8

		binary.LittleEndian.PutUint64(buf[pos:pos+8], uint64(r.Offset))
		pos += 8

		valLen := len(r.RawValue)
		binary.LittleEndian.PutUint32(buf[pos:pos+4], uint32(valLen))
		pos += 4
		copy(buf[pos:pos+valLen], r.RawValue)
	}

	return buf, nil
}

func (r *LogRecord) UnmarshalBinary(data []byte) error {
	if len(data) < 12 {
		return fmt.Errorf("log record data too short")
	}
	pos := 0

	r.Type = RecordType(binary.LittleEndian.Uint32(data[pos : pos+4]))
	pos += 4
	r.TxID = int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
	pos += 8

	if r.Type == TxSetBytes {
		if len(data) < pos+4 {
			return fmt.Errorf("malformed log record: cannot read filename length")
		}
		fnLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4

		if len(data) < pos+fnLen+8+8+4 {
			return fmt.Errorf("malformed log record: data truncated before fields")
		}
		r.Block.Filename = string(data[pos : pos+fnLen])
		pos += fnLen

		r.Block.Number = int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
		pos += 8

		r.Offset = int64(binary.LittleEndian.Uint64(data[pos : pos+8]))
		pos += 8

		valLen := int(binary.LittleEndian.Uint32(data[pos : pos+4]))
		pos += 4

		if len(data) < pos+valLen {
			return fmt.Errorf("malformed log record: truncated payload")
		}

		r.RawValue = make([]byte, valLen)
		copy(r.RawValue, data[pos:pos+valLen])
	}

	return nil
}

type LogManager struct {
	fileMgr      *FileManager
	logFile      string
	currentBlk   BlockID
	logPage      *Page
	blockSize    int64
	latestLSN    int64
	lastSavedLSN int64
	mu           sync.Mutex
}

func NewLogManager(fileMgr *FileManager, logFile string, blockSize int64) (*LogManager, error) {
	blocks, err := fileMgr.BlockCount(logFile)
	if err != nil {
		return nil, err
	}
	var (
		currentBlk BlockID
		p          = NewPage(blockSize)
	)
	if blocks == 0 {
		currentBlk, err = fileMgr.AppendBlock(logFile)
		if err != nil {
			return nil, err
		}
		err = p.PutInt32(0, int32(blockSize))
		if err != nil {
			return nil, err
		}
		err = fileMgr.WriteBlock(currentBlk, p)
		if err != nil {
			return nil, err
		}
	} else {
		currentBlk = BlockID{
			Number:   blocks - 1,
			Filename: logFile,
		}
		err = fileMgr.ReadBlock(currentBlk, p)
		if err != nil {
			return nil, err
		}
	}

	return &LogManager{
		fileMgr:      fileMgr,
		currentBlk:   currentBlk,
		logFile:      logFile,
		logPage:      p,
		blockSize:    blockSize,
		latestLSN:    0,
		lastSavedLSN: 0,
	}, nil
}

func (m *LogManager) unsafeAppendNewBlock() (BlockID, error) {
	blk, err := m.fileMgr.AppendBlock(m.logFile)
	if err != nil {
		return BlockID{}, err
	}
	err = m.logPage.PutInt32(0, int32(m.blockSize))
	if err != nil {
		return BlockID{}, err
	}
	err = m.fileMgr.WriteBlock(blk, m.logPage)
	return blk, err
}

func (m *LogManager) unsafeFlush() error {
	err := m.fileMgr.WriteBlock(m.currentBlk, m.logPage)
	if err != nil {
		return err
	}
	m.lastSavedLSN = m.latestLSN
	return nil
}

func (m *LogManager) AppendRecord(record LogRecord) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	data, err := record.MarshalBinary()
	if err != nil {
		return 0, err
	}
	boundary, err := m.logPage.GetInt32(0)
	if err != nil {
		return 0, err
	}

	const int32Size = 4
	bytesNeeded := len(data) + int32Size

	if boundary-int32(bytesNeeded) < int32Size {
		if err = m.unsafeFlush(); err != nil {
			return 0, err
		}
		m.currentBlk, err = m.unsafeAppendNewBlock()
		if err != nil {
			return 0, err
		}
		boundary, err = m.logPage.GetInt32(0)
		if err != nil {
			return 0, err
		}
	}

	pos := boundary - int32(bytesNeeded)
	if err = m.logPage.PutBytes(int64(pos), data); err != nil {
		return 0, err
	}
	if err = m.logPage.PutInt32(0, pos); err != nil {
		return 0, err
	}
	m.latestLSN += 1
	return m.latestLSN, nil
}

func (m *LogManager) Flush(lsn int64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if lsn > m.lastSavedLSN {
		return m.unsafeFlush()
	}
	return nil
}

func (m *LogManager) Iterator() iter.Seq2[LogRecord, error] {
	return func(yield func(LogRecord, error) bool) {
		m.mu.Lock()
		if err := m.unsafeFlush(); err != nil {
			m.mu.Unlock()
			var r LogRecord
			yield(r, err)
			return
		}
		currentBlk := m.currentBlk
		blockSize := m.blockSize
		m.mu.Unlock()

		p := NewPage(blockSize)

		for {
			if err := m.fileMgr.ReadBlock(currentBlk, p); err != nil {
				yield(LogRecord{}, err)
				return
			}
			boundary, boundaryErr := p.GetInt32(0)
			if boundaryErr != nil {
				yield(LogRecord{}, boundaryErr)
				return
			}

			currentPos := int64(boundary)

			for currentPos < blockSize {
				data, err := p.GetBytes(currentPos)
				if err != nil {
					yield(LogRecord{}, err)
					return
				}
				var r LogRecord
				if err = r.UnmarshalBinary(data); err != nil {
					yield(LogRecord{}, err)
					return
				}

				if !yield(r, nil) {
					return
				}
				currentPos += 4 + int64(len(data))
			}

			if currentBlk.Number == 0 {
				return
			}

			currentBlk = BlockID{
				Number:   currentBlk.Number - 1,
				Filename: currentBlk.Filename,
			}
		}
	}
}
