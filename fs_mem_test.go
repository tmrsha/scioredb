package scioredb

import (
	"bytes"
	"errors"
	"os"
	"sync"
	"testing"
)

func TestMemFS(t *testing.T) {
	t.Run("lazy file creation and core I/O", func(t *testing.T) {
		fsys := MemFS()

		filename := "test_table.tbl"
		f1, err := fsys.Open(filename)
		if err != nil {
			t.Fatalf("fsys.Open(%q) failed: %v", filename, err)
		}
		size, err := f1.Size()
		if err != nil {
			t.Fatalf("f1.Size() failed: %v", err)
		}
		if size != 0 {
			t.Errorf("new file size = %d, want 0", size)
		}
		wantData := []byte("simpledb_data")
		off := int64(10)

		if _, err := f1.WriteAt(wantData, off); err != nil {
			t.Fatalf("f1.WriteAt(...) failed: %v", err)
		}

		p := make([]byte, len(wantData))
		if _, err := f1.ReadAt(p, off); err != nil {
			t.Fatalf("f1.ReadAt(...) failed: %v", err)
		}

		if !bytes.Equal(p, wantData) {
			t.Errorf("f1.ReadAt(...) read %q, want %q", p, wantData)
		}
	})

	t.Run("descriptor isolation and concurrent shared access", func(t *testing.T) {
		fsys := MemFS()
		filename := "shared_log.log"
		f1, err := fsys.Open(filename)
		if err != nil {
			t.Fatalf("first open failed: %v", err)
		}
		f2, err := fsys.Open(filename)
		if err != nil {
			t.Fatalf("second open failed: %v", err)
		}
		writeData := []byte("transaction_record")
		if _, err = f1.WriteAt(writeData, 0); err != nil {
			t.Fatalf("f1.WriteAt failed: %v", err)
		}
		p := make([]byte, len(writeData))
		if _, err = f2.ReadAt(p, 0); err != nil {
			t.Fatalf("f2.ReadAt failed: %v", err)
		}
		if !bytes.Equal(p, writeData) {
			t.Errorf("f2 read %q, want %q", p, writeData)
		}
		if err = f1.Close(); err != nil {
			t.Fatalf("f1.Close() failed: %v", err)
		}
		if _, err = f1.ReadAt(p, 0); !errors.Is(err, os.ErrClosed) {
			t.Errorf("f1.ReadAt after close: got err = %v, want %v", err, os.ErrClosed)
		}
		if _, err = f2.ReadAt(p, 0); err != nil {
			t.Errorf("f2.ReadAt should work after f1 is closed, but got err: %v", err)
		}
	})

	t.Run("testing hook sync counter", func(t *testing.T) {
		fsys := MemFS()
		filename := "wal.log"

		f, err := fsys.Open(filename)
		if err != nil {
			t.Fatalf("fsys.Open failed: %v", err)
		}

		for range 3 {
			if err = f.Sync(); err != nil {
				t.Fatalf("f.Sync() failed: %v", err)
			}
		}

		rawFS, ok := fsys.(*memFS)
		if !ok {
			t.Fatalf("cannot cast fsys to *memFS")
		}

		rawFS.mu.RLock()
		memFileObj := rawFS.files[filename]
		rawFS.mu.RUnlock()

		if memFileObj == nil {
			t.Fatalf("internal memFile object not found in files map")
		}

		gotSyncs := memFileObj.syncCount.Load()
		if gotSyncs != 3 {
			t.Errorf("memFile.syncCount = %d, want 3", gotSyncs)
		}
	})

	t.Run("high concurrency race test", func(t *testing.T) {
		fsys := MemFS()
		filename := "heavy_load.db"

		var wg sync.WaitGroup
		goroutinesCount := 50

		for i := range goroutinesCount {
			wg.Add(1)
			go func(id int) {
				defer wg.Done()
				f, err := fsys.Open(filename)
				if err != nil {
					t.Errorf("concurrent open failed for goroutine %d: %v", id, err)
					return
				}
				off := int64(id * 100)
				data := []byte("race_test")

				if _, err = f.WriteAt(data, off); err != nil {
					t.Errorf("concurrent write failed for goroutine %d: %v", id, err)
				}
				_ = f.Close()
			}(i)
		}
		wg.Wait()
	})
}
