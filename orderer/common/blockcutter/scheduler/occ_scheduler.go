package scheduler

type Scheduler interface {
	ProcessTxn(readSets, writeSets []string, snapshot, nextCommittedHeight uint64, txnID string) bool
	ProcessBlk(committedHeight uint64) []string
}
