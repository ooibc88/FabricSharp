package microexp

import (
	"errors"
	"ustore"
)

type ProvUstoreDB struct {
	db ustore.KVDB
}

func NewProvUstoreDB() ProvDB {
	kvDB := ustore.NewKVDB()
	if status := kvDB.InitGlobalState(); !status.Ok() {
		panic("Fail to init the global state...")
	}
	return &ProvUstoreDB{db: kvDB}
}

func (udb *ProvUstoreDB) Put(key, value, txnID string, blk uint64, depKeys []string) error {
	vecStr := ustore.NewVecStr()
	for _, depKey := range depKeys {
		vecStr.Add(depKey)
	}
	// "NA" implies that we ignore the state snapshot for the dependency.
	// In other words, the dependency is always on the latest state.
	if ok := udb.db.PutState(key, value, txnID, blk, vecStr, "NA"); !ok {
		return errors.New("can't put key " + key + " with value " + value)
	}
	return nil
}

func (udb *ProvUstoreDB) Hist(key string, blk uint64) (string, uint64, error) {
	if histReturn := udb.db.Hist(key, blk); !histReturn.Status().Ok() {
		return "", 0, errors.New("Fail for the historical query on key " + key + " for blk " + string(blk))
	} else {
		blk := histReturn.Blk_idx()
		value := histReturn.Value()
		return value, blk, nil
	}
}

func (udb *ProvUstoreDB) Backward(key string, blk uint64) (string, []string, []uint64, error) {
	if backReturn := udb.db.Backward(key, blk); !backReturn.Status().Ok() {
		return "", nil, nil, errors.New("Fail for the backward query on key " + key + " for blk " + string(blk))
	} else {
		txnID := backReturn.TxnID()
		depVecKeys := backReturn.Dep_keys()
		depVecBlks := backReturn.Dep_blk_idx()

		depKeys := make([]string, 0)
		depBlks := make([]uint64, 0)
		for i := 0; i < int(depVecKeys.Size()); i++ {
			depKeys = append(depKeys, depVecKeys.Get(i))
			depBlks = append(depBlks, depVecBlks.Get(i))
		}
		return txnID, depKeys, depBlks, nil
	}
}

func (udb *ProvUstoreDB) Forward(key string, blk uint64) ([]string, []string, []uint64, error) {
	if forwardReturn := udb.db.Forward(key, blk); !forwardReturn.Status().Ok() {
		return nil, nil, nil, errors.New("Fail for the forward query on key " + key + " for blk " + string(blk))
	} else {
		txnVecIDs := forwardReturn.TxnIDs()
		antiDepVecKeys := forwardReturn.Forward_keys()
		antiDepVecBlks := forwardReturn.Forward_blk_idx()

		txnIDs := make([]string, 0)
		antiDepKeys := make([]string, 0)
		antiDepBlks := make([]uint64, 0)
		for i := 0; i < int(txnVecIDs.Size()); i++ {
			txnIDs = append(txnIDs, txnVecIDs.Get(i))
			antiDepKeys = append(antiDepKeys, antiDepVecKeys.Get(i))
			antiDepBlks = append(antiDepBlks, antiDepVecBlks.Get(i))
		}
		return txnIDs, antiDepKeys, antiDepBlks, nil

	}
}

func (udb *ProvUstoreDB) Commit() (string, error) {
	if statusStr := udb.db.Commit(); !statusStr.GetFirst().Ok() {
		return "", errors.New("Fail to commit")
	} else {
		return statusStr.GetSecond(), nil
	}
}

func (udb *ProvUstoreDB) Scan(key string) {
	udb.db.IterateState(key)
}
