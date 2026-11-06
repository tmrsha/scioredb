package scioredb

import (
	"bytes"
	"testing"
)

func TestLogRecord(t *testing.T) {
	t.Run("Marshal and Unmarshal Success Paths", func(t *testing.T) {
		tests := []struct {
			name string
			orig LogRecord
		}{
			{
				name: "Service Record START",
				orig: LogRecord{
					Type:  TxStart,
					TxID:  42,
					Block: BlockID{},
				},
			},
			{
				name: "Service Record COMMIT With Max TxID",
				orig: LogRecord{
					Type: TxCommit,
					TxID: 9223372036854775807,
				},
			},
			{
				name: "Data Modification Record SET_BYTES",
				orig: LogRecord{
					Type: TxSetBytes,
					TxID: 105,
					Block: BlockID{
						Filename: "test_table.dat",
						Number:   5555555555,
					},
					Offset:   8888888888,
					RawValue: []byte("hello_world_from_scioredb_wal_record"),
				},
			},
			{
				name: "SET_BYTES With Empty Slice",
				orig: LogRecord{
					Type: TxSetBytes,
					TxID: 106,
					Block: BlockID{
						Filename: "empty.dat",
						Number:   0,
					},
					Offset:   0,
					RawValue: []byte{},
				},
			},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				data, err := tt.orig.MarshalBinary()
				if err != nil {
					t.Fatalf("MarshalBinary failed: %v", err)
				}

				if tt.orig.Type != TxSetBytes && len(data) != 12 {
					t.Errorf("expected size 12 for service record, got: %d", len(data))
				}

				var res LogRecord
				err = res.UnmarshalBinary(data)
				if err != nil {
					t.Fatalf("UnmarshalBinary failed: %v", err)
				}

				if res.Type != tt.orig.Type {
					t.Errorf("type mismatch: expected %s, got %s", tt.orig.Type, res.Type)
				}
				if res.TxID != tt.orig.TxID {
					t.Errorf("txID mismatch: expected %d, got %d", tt.orig.TxID, res.TxID)
				}

				if tt.orig.Type == TxSetBytes {
					if res.Block.Filename != tt.orig.Block.Filename {
						t.Errorf("filename mismatch: expected %q, got %q", tt.orig.Block.Filename, res.Block.Filename)
					}
					if res.Block.Number != tt.orig.Block.Number {
						t.Errorf("block number mismatch: expected %d, got %d", tt.orig.Block.Number, res.Block.Number)
					}
					if res.Offset != tt.orig.Offset {
						t.Errorf("offset mismatch: expected %d, got %d", tt.orig.Offset, res.Offset)
					}
					if !bytes.Equal(res.RawValue, tt.orig.RawValue) {
						t.Errorf("rawValue mismatch: expected %v, got %v", tt.orig.RawValue, res.RawValue)
					}

					if len(tt.orig.RawValue) > 0 {
						tt.orig.RawValue[0] = 0xFF
						if bytes.Equal(res.RawValue, tt.orig.RawValue) {
							t.Error("defensive copy check failed: modifying original buffer altered unmarshaled result")
						}
					}
				}
			})
		}
	})

	t.Run("Unmarshal Too Short Header Data", func(t *testing.T) {
		var res LogRecord
		shortData := make([]byte, 11)
		err := res.UnmarshalBinary(shortData)
		if err == nil {
			t.Error("expected error for too short buffer, got nil")
		}
	})

	t.Run("Unmarshal Malformed SetBytes Record", func(t *testing.T) {
		orig := LogRecord{
			Type: TxSetBytes,
			TxID: 1,
		}
		data, _ := orig.MarshalBinary()
		truncatedData := data[:12]

		var res LogRecord
		err := res.UnmarshalBinary(truncatedData)
		if err == nil {
			t.Error("expected error for truncated SET_BYTES record, got nil")
		}
	})
}

func TestLogManager(t *testing.T) {
	logFile := "log.log"
	blockSize := int64(160)
	fs := MemFS()
	fileMgr := NewFileManager(fs, blockSize)
	logMgr, err := NewLogManager(fileMgr, logFile, blockSize)
	if err != nil {
		t.Fatalf("failed to create log manager: %v", err)
	}
	records := []LogRecord{
		{
			Type: TxStart,
			TxID: 1,
		},
		{
			Type: TxSetBytes,
			TxID: 1,
			Block: BlockID{
				Filename: "wal.log",
				Number:   10,
			},
			Offset:   20,
			RawValue: []byte("first_test_value_payload"),
		},
		{
			Type: TxStart,
			TxID: 2,
		},
		{
			Type: TxSetBytes,
			TxID: 2,
			Block: BlockID{
				Filename: "wal.log",
				Number:   11,
			},
			Offset:   40,
			RawValue: []byte("second_test_value_payload_large_enough_to_trigger_block_append"),
		},
		{
			Type: TxCommit,
			TxID: 1,
		},
	}

	var lastLSN int64
	for i, rec := range records {
		lsn, err := logMgr.AppendRecord(rec)
		if err != nil {
			t.Fatalf("failed to append record at index %d: %v", i, err)
		}
		if lsn <= lastLSN {
			t.Errorf("expected LSN to strictly increase, got %d after %d", lsn, lastLSN)
		}
		lastLSN = lsn
	}

	if err := logMgr.Flush(lastLSN); err != nil {
		t.Fatalf("failed to flush log up to LSN %d: %v", lastLSN, err)
	}

	count, err := fileMgr.BlockCount(logFile)
	if err != nil {
		t.Fatalf("failed to get log block count: %v", err)
	}
	if count < 2 {
		t.Errorf("expected log to span across multiple blocks, got count: %d", count)
	}

	var iteratedRecords []LogRecord
	for rec, err := range logMgr.Iterator() {
		if err != nil {
			t.Fatalf("iterator returned an error: %v", err)
		}
		iteratedRecords = append(iteratedRecords, rec)
	}

	if len(iteratedRecords) != len(records) {
		t.Fatalf("expected %d records during iteration, got %d", len(records), len(iteratedRecords))
	}

	for i := range records {
		expected := records[len(records)-1-i]
		actual := iteratedRecords[i]

		if actual.Type != expected.Type {
			t.Errorf("index %d: type mismatch: expected %s, got %s", i, expected.Type, actual.Type)
		}
		if actual.TxID != expected.TxID {
			t.Errorf("index %d: txID mismatch: expected %d, got %d", i, expected.TxID, actual.TxID)
		}

		if expected.Type == TxSetBytes {
			if actual.Block.Filename != expected.Block.Filename {
				t.Errorf("index %d: filename mismatch: expected %q, got %q", i, expected.Block.Filename, actual.Block.Filename)
			}
			if actual.Block.Number != expected.Block.Number {
				t.Errorf("index %d: block number mismatch: expected %d, got %d", i, expected.Block.Number, actual.Block.Number)
			}
			if actual.Offset != expected.Offset {
				t.Errorf("index %d: offset mismatch: expected %d, got %d", i, expected.Offset, actual.Offset)
			}
			if !bytes.Equal(actual.RawValue, expected.RawValue) {
				t.Errorf("index %d: rawValue mismatch: expected %v, got %v", i, expected.RawValue, actual.RawValue)
			}
		}
	}

	newLogMgr, err := NewLogManager(fileMgr, logFile, blockSize)
	if err != nil {
		t.Fatalf("failed to restart log manager: %v", err)
	}

	var recoveryRecords []LogRecord
	for rec, err := range newLogMgr.Iterator() {
		if err != nil {
			t.Fatalf("recovery iterator returned an error: %v", err)
		}
		recoveryRecords = append(recoveryRecords, rec)
	}

	if len(recoveryRecords) != len(records) {
		t.Errorf("expected %d records after restart recovery scan, got %d", len(records), len(recoveryRecords))
	}
}
