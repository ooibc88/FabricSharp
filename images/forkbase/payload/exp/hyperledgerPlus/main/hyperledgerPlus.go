package main

import (
	"fmt"
	"microexp"
	"os"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Println("Invalid parameter. Should be  [scan|query|bfs]")
		fmt.Println("   e.g, './hyperledgerPlus bfs' will reproduce experiment in Figure 10b")
		return
	}
	dbPath := "/tmp/rocksDBDataDir"
	if err := os.RemoveAll(dbPath); err != nil {
		panic("Fail to clean db persistent data...")
	}
	db := microexp.NewProvRocksDB(dbPath)

	expType := os.Args[1]
	if expType == "scan" {
		microexp.VersionScan(db) // For Figure 11c in paper
	} else if expType == "query" {
		microexp.VersionQuery(db) // For Figure 11a and b
	} else if expType == "bfs" {
		microexp.SupplyExp(db) // For Figure 10b
	} else {
		fmt.Println("Unsupported experimental type " + expType)
	}
}
