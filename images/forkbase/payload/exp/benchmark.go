package microexp

// Must be correctly set according to the ustore configuration
// ,data_dir in ${USTORE_HOME}/build/conf/config.cfg
// const ustoreDataDir = "/data/ruanpc/ustore_data"
// const rocksDBDataDir = "/tmp/rocksdb"
const blkSize = 500
const keyCount = 500

// For Figure 11c
func VersionScan(db ProvDB) {
	blkCount := 1025
	operationCount := blkCount * blkSize
	bm := NewYCSBBenchmark(db, keyCount, operationCount, blkSize)
	bm.HistLoad()
	bm.MeasureScan()
}

// For Figure 11a and b
func VersionQuery(db ProvDB) {
	blkCount := 1025
	// blkCount := 2049
	// blkCount := 4097
	// blkCount := 8193
	operationCount := blkCount * blkSize
	bm := NewYCSBBenchmark(db, keyCount, operationCount, blkSize)
	bm.HistLoad()
	blkDistances := []uint64{0, 1, 2, 4, 8, 16, 32, 64, 128, 256, 512}
	bm.MeasureHistQuery(blkDistances)
}

// For Figure 10b
func SupplyExp(db ProvDB) {
	pm := NewProvBenchmark(db, 500)
	pm.SupplychainLoad()
	pm.BFSMeasure(1)
	pm.BFSMeasure(2)
	pm.BFSMeasure(3)
	pm.BFSMeasure(4)
	pm.BFSMeasure(5)
	pm.BFSMeasure(6)
}

// func main() {
// 	if len(os.Args) < 3 {
// 		fmt.Println("Invalid parameter. Should be [forkbase|rocksdb] [scan|query|bfs]")
// 		fmt.Println("   e.g, './main forkbase bfs' will reproduce experiment in Figure 10b")
// 		return
// 	}
// 	dbType := os.Args[1]
// 	var dbPath string
// 	var db microexp.ProvDB
// 	if dbType == "rocksdb" {
// 		dbPath = rocksDBDataDir
// 		if err := os.RemoveAll(dbPath); err != nil {
// 			panic("Fail to clean db persistent data...")
// 		}
// 		db = microexp.NewProvRocksDB(dbPath)
// 	} else if dbType == "forkbase" {
// 		dbPath = ustoreDataDir
// 		if err := os.RemoveAll(dbPath); err != nil {
// 			panic("Fail to clean db persistent data...")
// 		}
// 		db = microexp.NewProvUstoreDB()
// 	} else {
// 		fmt.Printf("Unrecognized dbtype %s \n", dbType)
// 		return
// 	}

// 	expType := os.Args[2]
// 	if expType == "scan" {
// 		VersionScan(db) // For Figure 11c in paper
// 	} else if expType == "query" {
// 		VersionQuery(db) // For Figure 11a and b
// 	} else if expType == "bfs" {
// 		SupplyExp(db) // For Figure 10b
// 	}
// }
