package scioredb

import "sync/atomic"

type IDGenerator struct {
	value atomic.Int64
}

func (gen *IDGenerator) NextID() int64 {
	return gen.value.Add(1)
}

// SetMaxID sets the initial maximum value of the counter.
//
// WARNING: This method is intended EXCLUSIVELY for the crash recovery phase
// when the global Recover() function computes the highest TxID present in the WAL.
// It must be executed strictly in a single-threaded execution context BEFORE the
// database engine opens its doors to accept or create new user transactions.
func (gen *IDGenerator) SetMaxID(i int64) {
	gen.value.Store(i)
}
