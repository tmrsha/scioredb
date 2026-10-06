package scioredb

import (
	"iter"
	"sync"
)

type LogManager struct {
	fm           *FileManager
	logFile      string
	currentBlk   BlockID
	logPage      *Page
	blockSize    int64
	latestLSN    int64
	lastSavedLSN int64
	mu           sync.Mutex
}

func NewLogManager(fm *FileManager, logFile string, blockSize int64) (*LogManager, error) {
	blocks, err := fm.BlockCount(logFile)
	if err != nil {
		return nil, err
	}
	var (
		currentBlk BlockID
		p          = NewPage(blockSize)
	)
	if blocks == 0 {
		currentBlk, err = fm.AppendBlock(logFile)
		if err != nil {
			return nil, err
		}
		err = p.PutInt32(0, int32(blockSize))
		if err != nil {
			return nil, err
		}
		err = fm.WriteBlock(currentBlk, p)
		if err != nil {
			return nil, err
		}
	} else {
		currentBlk = BlockID{
			Number:   blocks - 1,
			Filename: logFile,
		}
		err = fm.ReadBlock(currentBlk, p)
		if err != nil {
			return nil, err
		}
	}
	return &LogManager{
		fm:         fm,
		currentBlk: currentBlk,
		logFile:    logFile,
		logPage:    p,
		blockSize:  blockSize,
	}, nil
}

func (m *LogManager) appendNewBlock() (BlockID, error) {
	blk, err := m.fm.AppendBlock(m.logFile)
	if err != nil {
		return BlockID{}, err
	}
	err = m.logPage.PutInt32(0, int32(m.blockSize))
	if err != nil {
		return BlockID{}, err
	}
	err = m.fm.WriteBlock(blk, m.logPage)
	return blk, err
}

func (m *LogManager) flush() error {
	err := m.fm.WriteBlock(m.currentBlk, m.logPage)
	if err != nil {
		return err
	}
	m.lastSavedLSN = m.latestLSN
	return nil
}

func (m *LogManager) AppendRecord(record []byte) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	boundary, err := m.logPage.GetInt32(0)
	if err != nil {
		return 0, err
	}
	bytesNeeded := len(record) + int32Size
	if boundary-int32(bytesNeeded) < int32Size {
		if err = m.flush(); err != nil {
			return 0, err
		}
		m.currentBlk, err = m.appendNewBlock()
		if err != nil {
			return 0, err
		}
		boundary, err = m.logPage.GetInt32(0)
		if err != nil {
			return 0, err
		}
	}
	pos := boundary - int32(bytesNeeded)
	if err = m.logPage.PutBytes(int64(pos), record); err != nil {
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
		return m.flush()
	}
	return nil
}

func (m *LogManager) Iterator() iter.Seq2[[]byte, error] {
	return func(yield func([]byte, error) bool) {
		m.mu.Lock()
		currentBlk := m.currentBlk
		blockSize := m.blockSize
		m.mu.Unlock()
		p := NewPage(blockSize)
		for {
			if err := m.fm.ReadBlock(currentBlk, p); err != nil {
				yield(nil, err)
				return
			}

			boundary, err := p.GetInt32(0)
			if err != nil {
				yield(nil, err)
				return
			}

			currentPos := int64(boundary)

			for currentPos < blockSize {
				record, err := p.GetBytes(currentPos)
				if err != nil {
					yield(nil, err)
					return
				}
				if !yield(record, nil) {
					return
				}
				currentPos += int64Size/2 + int64(len(record))
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
