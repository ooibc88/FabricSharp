package microexp

type ProvDB interface {
	Put(key, value, txnID string, blk uint64, depKeys []string) error
	Hist(key string, blk uint64) (string, uint64, error)
	Backward(key string, blk uint64) (string, []string, []uint64, error)
	Forward(key string, blk uint64) ([]string, []string, []uint64, error)
	Commit() (string, error)
	Scan(key string)
}
