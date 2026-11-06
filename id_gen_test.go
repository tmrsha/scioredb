package scioredb

import (
	"sync"
	"testing"
)

func TestIDGenerator(t *testing.T) {
	t.Run("NextID_Concurrency", func(t *testing.T) {
		gen := &IDGenerator{}

		const goroutines = 10
		const incrementsPerGoroutine = 1000
		totalExpected := int64(goroutines * incrementsPerGoroutine)

		var wg sync.WaitGroup
		var generatedIDs sync.Map

		wg.Add(goroutines)
		for range goroutines {
			go func() {
				defer wg.Done()
				for range incrementsPerGoroutine {
					id := gen.NextID()

					if _, loaded := generatedIDs.LoadOrStore(id, true); loaded {
						t.Errorf("critical invariant broken: duplicate ID generated: %d", id)
					}
				}
			}()
		}
		wg.Wait()

		finalID := gen.NextID() - 1
		if finalID != totalExpected {
			t.Errorf("unexpected final ID counter value: got %d, want %d", finalID, totalExpected)
		}
	})

	t.Run("SetMaxID_RecoverySimulation", func(t *testing.T) {
		gen := &IDGenerator{}

		gen.NextID()
		gen.NextID()
		lastTxIDBeforeCrash := gen.NextID()

		if lastTxIDBeforeCrash != 3 {
			t.Fatalf("setup failed: unexpected initial ID: got %d, want 3", lastTxIDBeforeCrash)
		}

		maxTxIDFoundInWAL := lastTxIDBeforeCrash
		nextValidIDAfterRecovery := maxTxIDFoundInWAL + 1

		gen.SetMaxID(nextValidIDAfterRecovery)

		firstNewTxID := gen.NextID()
		if firstNewTxID != 5 {
			t.Errorf("recovery sync failed: first ID after SetMaxID got %d, want 5", firstNewTxID)
		}

		secondNewTxID := gen.NextID()
		if secondNewTxID != 6 {
			t.Errorf("monotonic invariant broken: second ID after SetMaxID got %d, want 6", secondNewTxID)
		}
	})
}
