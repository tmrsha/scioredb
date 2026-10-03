package scioredb

import (
	"bytes"
	"errors"
	"fmt"

	"sync"
	"testing"
)

func TestFileManager(t *testing.T) {
	memFS := MemFS()
	blockSize := int64(400)
	fm := NewFileManager(memFS, blockSize)
	testFile := "test_database.db"

	t.Run("HappyPath", func(t *testing.T) {
		count, err := fm.BlockCount(testFile)
		if err != nil {
			t.Fatalf("unexpected error getting initial block count: %v", err)
		}
		if count != 0 {
			t.Errorf("expected 0 blocks, got %d", count)
		}
		blk, err := fm.AppendBlock(testFile)
		if err != nil {
			t.Fatalf("failed to append block: %v", err)
		}
		if blk.Number != 0 || blk.Filename != testFile {
			t.Errorf("unexpected BlockID: %+v", blk)
		}
		count, err = fm.BlockCount(testFile)
		if err != nil || count != 1 {
			t.Errorf("expected 1 block, got %d (err: %v)", count, err)
		}
		pageWrite := NewPage(blockSize)
		copy(pageWrite.Bytes(), []byte("hello simpledb"))
		if err := fm.WriteBlock(blk, pageWrite); err != nil {
			t.Fatalf("failed to write block: %v", err)
		}
		pageRead := NewPage(blockSize)
		if err := fm.ReadBlock(blk, pageRead); err != nil {
			t.Fatalf("failed to read block: %v", err)
		}
		if !bytes.Equal(pageRead.Bytes()[:14], []byte("hello simpledb")) {
			t.Errorf("read bytes mismatch, got: %s", pageRead.Bytes()[:14])
		}
	})

	t.Run("Concurrency", func(t *testing.T) {
		const goroutinesCount = 50
		var wg sync.WaitGroup
		errChan := make(chan error, goroutinesCount)

		wg.Add(goroutinesCount)
		for i := 0; i < goroutinesCount; i++ {
			go func(workerID int) {
				defer wg.Done()
				_, err := fm.AppendBlock(testFile)
				if err != nil {
					errChan <- fmt.Errorf("worker %d failed to append: %w", workerID, err)
				}
			}(i)
		}
		wg.Wait()
		close(errChan)
		for err := range errChan {
			t.Errorf("concurrency error: %v", err)
		}
		count, err := fm.BlockCount(testFile)
		if err != nil {
			t.Fatalf("failed to get block count after load: %v", err)
		}
		if count != 51 {
			t.Errorf("expected 51 blocks in total, got %d. Race condition detected!", count)
		}
	})

	t.Run("CloseAndGuard", func(t *testing.T) {
		if err := fm.Close(); err != nil {
			t.Fatalf("failed to close file manager: %v", err)
		}
		blk := BlockID{Number: 0, Filename: testFile}
		page := NewPage(blockSize)

		err := fm.ReadBlock(blk, page)
		if !errors.Is(err, ErrFileManagerClosed) {
			t.Errorf("expected error %v, got %v", ErrFileManagerClosed, err)
		}

		_, err = fm.AppendBlock(testFile)
		if !errors.Is(err, ErrFileManagerClosed) {
			t.Errorf("expected error %v on append, got %v", ErrFileManagerClosed, err)
		}

		err = fm.Close()
		if !errors.Is(err, ErrFileManagerClosed) {
			t.Errorf("expected error %v on double close, got %v", ErrFileManagerClosed, err)
		}
	})
}
