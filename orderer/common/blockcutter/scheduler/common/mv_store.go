package common

import (
	"bytes"
	"fmt"

	"github.com/hyperledger/fabric/common/ledger/util"
	"github.com/syndtr/goleveldb/leveldb"
)

type Mvstore struct {
	db *leveldb.DB
}

var WRITE_PREFIX = []byte{0x01}
var READ_PREFIX = []byte{0x10}
var DELIMITER = []byte{0x00}
var EMPTY = []byte{}

func NewMvStore(path string) *Mvstore {
	if db, err := leveldb.OpenFile(path, nil); err != nil {
		panic("Fail to open leveldb at path " + path + " with err msg " + err.Error())
	} else {
		return &Mvstore{db: db}
	}
}

func parseCompositeKey(raw []byte) (opType []byte, key string, blk uint64, seq int, err error) {
	// Pass 10 to deliberate try to split more
	if splits := bytes.SplitN(raw, DELIMITER, 3); len(splits) != 3 {
		err = fmt.Errorf("fail to split %v into four elements...", raw)
	} else {
		opType = splits[0]
		key = string(splits[1])
		blkSeq, _, _ := util.DecodeOrderPreservingVarUint64(splits[2])
		blk = blkSeq >> 10
		seq = int(blkSeq & (1<<10 - 1))
	}
	return

}

func suffixCompositeKey(key string, blk uint64, seq int) []byte {
	if 1<<10 <= seq {
		panic("Seq must be smaller than 1024")
	}
	var partialCompositeKey []byte
	partialCompositeKey = append(partialCompositeKey, []byte(key)...)
	partialCompositeKey = append(partialCompositeKey, DELIMITER...)

	// Since the encoded bytes for uint64 could also contain DELIMITER,
	// hence the integer encoded bytes can not be put before any DELIMITER
	// We have to encode two integers together to address this issue.
	// To do so, we assume seq < 1024.
	// we use the lower ten bits to encode seq and higher ordered bits to encode blk
	blkSeq := blk<<10 + uint64(seq)
	partialCompositeKey = append(partialCompositeKey, util.EncodeOrderPreservingVarUint64(blkSeq)...)
	return partialCompositeKey
}

func writeCompositeKey(key string, blk uint64, seq int) []byte {
	var writeCompositeKey []byte
	writeCompositeKey = append(writeCompositeKey, WRITE_PREFIX...)
	writeCompositeKey = append(writeCompositeKey, DELIMITER...)
	writeCompositeKey = append(writeCompositeKey, suffixCompositeKey(key, blk, seq)...)
	return writeCompositeKey
}

func readCompositeKey(key string, blk uint64, seq int) []byte {
	var readCompositeKey []byte
	readCompositeKey = append(readCompositeKey, READ_PREFIX...)
	readCompositeKey = append(readCompositeKey, DELIMITER...)
	readCompositeKey = append(readCompositeKey, suffixCompositeKey(key, blk, seq)...)
	return readCompositeKey
}

func (s *Mvstore) Commit(blkHeight uint64, updates, reads map[string][]string) error {
	batch := new(leveldb.Batch)

	for key, txns := range updates {
		for seq, txn := range txns {
			// fmt.Printf("Put write key %s, %d, %d\n", key, blkHeight, seq)
			batch.Put(writeCompositeKey(key, blkHeight, seq), []byte(txn))
		}
		// TODO: delete all previous read records for key.
		// Mark this txn overrides the key
		batch.Put(readCompositeKey(key, blkHeight, 0), EMPTY)
	}

	for key, txns := range reads {
		for seq, txn := range txns {
			// seq = 0 has the special meaning as above
			batch.Put(readCompositeKey(key, blkHeight, seq+1), []byte(txn))
		}
	}

	return s.db.Write(batch, nil)
}

func (s *Mvstore) BatchUpdate(updates map[string]string) error {
	batch := new(leveldb.Batch)
	for key, value := range updates {
		batch.Put([]byte(key), []byte(value))
	}
	return s.db.Write(batch, nil)
}

func (s *Mvstore) Get(key string) string {
	if val, err := s.db.Get([]byte(key), nil); err == leveldb.ErrNotFound {
		return ""
	} else if err != nil {
		panic("LevelDB get fails with error " + err.Error())
	} else {
		return string(val)
	}
}

func (s *Mvstore) LastUpdatedTxnNoLaterThanBlk(blkHeight uint64, key string, isValid func(txn string) bool) (txn string, found bool) {
	it := s.db.NewIterator(nil, nil)
	if it.Seek(writeCompositeKey(key, blkHeight+1, 0)); it.Prev() {
		// Not found
		found = false
	}
	if actualOp, actualKey, actualBlk, _, err := parseCompositeKey(it.Key()); !(err == nil && bytes.Compare(actualOp, WRITE_PREFIX) == 0 && actualKey == key && actualBlk <= blkHeight) {
		found = false
	} else if updatedTxn := string(it.Value()); isValid(updatedTxn) {
		txn = updatedTxn
		found = true
	}
	it.Release()
	return
}

func (s *Mvstore) UpdatedTxnsNoEarlierThanBlk(blkHeight uint64, key string, isValid func(txn string) bool) (txns TxnSet) {
	txns = NewTxnSet()
	it := s.db.NewIterator(nil, nil)
	if exists := it.Seek(writeCompositeKey(key, blkHeight, 0)); !exists {
		return
	}
	for true {
		keyBytes := it.Key()
		actualType, actualKey, actualBlk, _, err := parseCompositeKey(keyBytes)
		if !(err == nil && bytes.Compare(actualType, WRITE_PREFIX) == 0 && actualKey == key && blkHeight <= actualBlk) {
			break
		}

		txn := string(it.Value())
		if isValid(txn) {
			txns.Add(txn)
		}
		if !it.Next() {
			break
		}
	}
	it.Release()
	return
}

func (s *Mvstore) ReadTxnsEarlierThanBlk(blkHeight uint64, key string, isValid func(txn string) bool) (txns TxnSet) {
	txns = NewTxnSet()
	it := s.db.NewIterator(nil, nil)
	it.Seek(readCompositeKey(key, blkHeight, 0))
	for it.Prev() {
		if actualOp, actualKey, actualBlk, actualSeq, err := parseCompositeKey(it.Key()); !(err == nil && bytes.Compare(actualOp, READ_PREFIX) == 0 && actualKey == key && actualBlk < blkHeight && actualSeq != 0) {
			break
		}
		if txn := string(it.Value()); isValid(txn) {
			txns.Add(txn)
		}
	}
	it.Release()
	return
}

func (s *Mvstore) ReadTxnsNoEarlierThanBlk(blkHeight uint64, key string) (txns TxnSet) {
	txns = NewTxnSet()
	it := s.db.NewIterator(nil, nil)
	if exists := it.Seek(readCompositeKey(key, blkHeight, 0)); !exists {
		return
	}

	for true {
		if actualOp, actualKey, actualBlk, actualSeq, err := parseCompositeKey(it.Key()); !(err == nil && bytes.Compare(actualOp, READ_PREFIX) == 0 && actualKey == key && blkHeight <= actualBlk) {
			break
		} else if actualSeq == 0 {
			txns = NewTxnSet() // ignore all preceding txns
		} else {
			txns.Add(string(it.Value()))
		}
		if !it.Next() {
			break
		}
	}
	it.Release()
	return
}

// TODO: Remove all updated records before or equal to earliestCommittedHeight
//       and read records preceding the tombstone
func (s *Mvstore) Clean(earliestCommittedHeight uint64) {
	// Can be done asyncly without degrading the performance.
	// Hence, temporarally not implemented
	panic("Not implemented for clean")
}

func (s *Mvstore) Close() {
	s.Close()
}
