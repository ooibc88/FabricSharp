/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package sharpscheduler

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSort(t *testing.T) {
	sorted := []string{"a", "b", "c", "d", "e"}
	toBeSorted := []string{"e", "b", "d"}

	sortedMap := make(map[string]int)
	for idx, element := range sorted {
		sortedMap[element] = idx
	}
	sort.Slice(toBeSorted, func(i, j int) bool {
		return sortedMap[toBeSorted[i]] < sortedMap[toBeSorted[j]]
	})
	assert.Equal(t, []string{"b", "d", "e"}, toBeSorted)

}

func ToMap(l []string) map[string]bool {
	lmap := make(map[string]bool)
	for _, val := range l {
		lmap[val] = true
	}
	return lmap
}

func TestScheduler1(t *testing.T) {
	path := "/tmp/ts1"
	os.RemoveAll(path)
	scheduler := createTxnScheduler(3, true, path)
	/////////////////////////////////
	// Fire txn0, which inits all the involved keys
	fmt.Println("===========================Start Block 0==========================")
	blkHeight := uint64(0)
	txn0ReadKeys := []string{}
	txn0WriteKeys := []string{"A", "B", "C", "D", "E", "F", "G"}
	txn0 := "tx0"
	fmt.Println("---Process Txn " + txn0)
	assert.True(t, scheduler.ProcessTxn(txn0ReadKeys, txn0WriteKeys, 0, 0, txn0))
	fmt.Println("++++++++++++++++++++Retrieve Schedule++++++++++++")
	assert.Equal(t, []string{txn0}, scheduler.ProcessBlk(blkHeight))
	fmt.Println("^^^^^^^^^^^^^^^^^Report Detail Stats^^^^^^^^^^^^^^^")
	scheduler.detailedStat(blkHeight)

	fmt.Println("\n\n\n\n===========================Start Block 1============================")
	blkHeight = 1
	txn1 := "tx1"
	txn1ReadKeys := []string{"A"}
	txn1WriteKeys := []string{"A", "B"}
	// tx0 -wr,ww-> tx1
	assert.True(t, scheduler.ProcessTxn(txn1ReadKeys, txn1WriteKeys, 0, 1, txn1))

	txn2 := "tx2"
	txn2ReadKeys := []string{"B"}
	txn2WriteKeys := []string{"A"}
	// CYCLE: tx1 -rw> tx2 -> tx1
	assert.False(t, scheduler.ProcessTxn(txn2ReadKeys, txn2WriteKeys, 0, 1, txn2))

	txn3 := "tx3"
	txn3ReadKeys := []string{"A", "G"}
	txn3WriteKeys := []string{"B"}
	// Reorderable Cycle: tx1 -rw> tx3 -ww*> tx1
	assert.True(t, scheduler.ProcessTxn(txn3ReadKeys, txn3WriteKeys, 0, 1, txn3))

	fmt.Println("++++++++++++++++++++Retrieve Schedule++++++++++++")
	assert.Equal(t, []string{txn3, txn1}, scheduler.ProcessBlk(blkHeight))
	fmt.Println("^^^^^^^^^^^^^^^^^Report Detail Stats^^^^^^^^^^^^^^^")
	scheduler.detailedStat(blkHeight)

	fmt.Println("\n\n\n\n===========================Start Block 2============================")
	blkHeight = 2
	txn4 := "tx4"
	txn4ReadKeys := []string{"A"}
	txn4WriteKeys := []string{"B"}
	// CYCLE (with inter-block ww conflict): tx1, tx2 -ww-> tx4 -rw-> tx1, tx2
	assert.False(t, scheduler.ProcessTxn(txn4ReadKeys, txn4WriteKeys, 0, blkHeight, txn4))

	txn5 := "tx5"
	txn5ReadKeys := []string{}
	txn5WriteKeys := []string{"A", "C"}
	// tx0, tx1 -ww-> tx5 (Key A, C)
	// tx1, tx3 -rw-> tx5 (Key A)
	assert.True(t, scheduler.ProcessTxn(txn5ReadKeys, txn5WriteKeys, 0, blkHeight, txn5))

	txn6 := "tx6"
	txn6ReadKeys := []string{"B"}
	txn6WriteKeys := []string{"C"}
	// Reorderable Cycle:
	// tx6 -rw-> tx1, tx3 (Key B)
	// tx1 -> tx5 (previous)
	// tx5 -ww-> tx6 (Key C) (Swappable)
	assert.True(t, scheduler.ProcessTxn(txn6ReadKeys, txn6WriteKeys, 0, blkHeight, txn6))

	fmt.Println("++++++++++++++++++++Retrieve Schedule++++++++++++")
	assert.Equal(t, []string{txn6, txn5}, scheduler.ProcessBlk(blkHeight))
	fmt.Println("^^^^^^^^^^^^^^^^^Report Detail Stats^^^^^^^^^^^^^^^")
	scheduler.detailedStat(blkHeight)

	fmt.Println("\n\n\n\n===========================Start Block 3============================")
	blkHeight = 3
	txn7 := "tx7"
	txn7ReadKeys := []string{"A"}
	txn7WriteKeys := []string{"E"}
	// tx1 -wr-> txn7 (Key A)
	// tx7 -rw-> txn5 (Key A)
	assert.True(t, scheduler.ProcessTxn(txn7ReadKeys, txn7WriteKeys, 1, blkHeight, txn7))

	txn8 := "tx8"
	txn8ReadKeys := []string{"A"}
	txn8WriteKeys := []string{"F"}
	// tx5 -> wr -> tx8 (Key A)
	assert.True(t, scheduler.ProcessTxn(txn8ReadKeys, txn8WriteKeys, 2, blkHeight, txn8))

	txn9 := "tx9"
	txn9ReadKeys := []string{}
	txn9WriteKeys := []string{"B"}
	// tx6 -> rw -> tx9 (Key B)
	assert.True(t, scheduler.ProcessTxn(txn9ReadKeys, txn9WriteKeys, 1, blkHeight, txn9))

	fmt.Println("++++++++++++++++++++Retrieve Schedule++++++++++++")
	assert.Equal(t, []string{txn7, txn9, txn8}, scheduler.ProcessBlk(blkHeight))
	fmt.Println("^^^^^^^^^^^^^^^^^Report Detail Stats^^^^^^^^^^^^^^^")
	scheduler.detailedStat(blkHeight)

	fmt.Println("\n\n\n\n===========================Start Block 4============================")
	blkHeight = 4
	txn10 := "tx10"
	txn10ReadKeys := []string{"E"}
	txn10WriteKeys := []string{"F"}
	// CYCLE:
	// tx5 -wr-> txn8 -ww->txn10 (Key E)
	// tx10 -rw-> txn7 (Key E) -rw-> txn5
	assert.False(t, scheduler.ProcessTxn(txn10ReadKeys, txn10WriteKeys, 2, blkHeight, txn10))

	txn11 := "tx11"
	txn11ReadKeys := []string{"C"}
	txn11WriteKeys := []string{"G"}
	// CYCLE:
	// tx11 -rw-> tx6 -rw-> tx3
	// tx3 -rw-> txn11 (Key G)
	assert.False(t, scheduler.ProcessTxn(txn11ReadKeys, txn11WriteKeys, 1, blkHeight, txn11))

	txn12 := "tx12"
	txn12ReadKeys := []string{}
	txn12WriteKeys := []string{"C"}
	// CYCLE:
	// tx5 -ww-> txn12 (Key C)
	assert.True(t, scheduler.ProcessTxn(txn12ReadKeys, txn12WriteKeys, 1, blkHeight, txn12))

	fmt.Println("++++++++++++++++++++Retrieve Schedule++++++++++++")
	assert.Equal(t, []string{txn12}, scheduler.ProcessBlk(blkHeight))
	fmt.Println("^^^^^^^^^^^^^^^^^Report Detail Stats^^^^^^^^^^^^^^^")
	scheduler.detailedStat(blkHeight)
}

func TestScheduler2(t *testing.T) {
	path := "/tmp/ts2"
	os.RemoveAll(path)
	scheduler := createTxnScheduler(5, true, path)
	fmt.Println("===========================Start Block 1==========================")
	blkHeight := uint64(1)
	txn0ReadKeys := []string{}
	txn0WriteKeys := []string{}
	for i := 0; i < 100; i++ {
		txn0WriteKeys = append(txn0WriteKeys, "acc"+strconv.Itoa(i))
	}

	txn0 := "0fd"
	fmt.Println("---Process Txn " + txn0)
	assert.True(t, scheduler.ProcessTxn(txn0ReadKeys, txn0WriteKeys, 1, 1, txn0))
	fmt.Println("++++++++++++++++++++Retrieve Schedule++++++++++++")
	assert.Equal(t, []string{txn0}, scheduler.ProcessBlk(blkHeight))
	fmt.Println("^^^^^^^^^^^^^^^^^Report Detail Stats^^^^^^^^^^^^^^^")
	scheduler.detailedStat(blkHeight)

	fmt.Println("\n\n\n\n===========================Start Block 2============================")
	blkHeight = 2
	txn1 := "40f"
	txn1ReadKeys := []string{"acc30", "acc90"}
	txn1WriteKeys := []string{"acc12", "acc17"}
	assert.True(t, scheduler.ProcessTxn(txn1ReadKeys, txn1WriteKeys, 1, 2, txn1))

	txn2 := "908"
	txn2ReadKeys := []string{"acc0", "acc97"}
	txn2WriteKeys := []string{"acc9", "acc96"}
	assert.True(t, scheduler.ProcessTxn(txn2ReadKeys, txn2WriteKeys, 1, 2, txn2))

	txn3 := "f43"
	txn3ReadKeys := []string{"acc21", "acc62"}
	txn3WriteKeys := []string{"acc0", "acc91"}
	assert.True(t, scheduler.ProcessTxn(txn3ReadKeys, txn3WriteKeys, 1, 2, txn3))

	txn4 := "778"
	txn4ReadKeys := []string{"acc39", "acc65"}
	txn4WriteKeys := []string{"acc28", "acc61"}
	assert.True(t, scheduler.ProcessTxn(txn4ReadKeys, txn4WriteKeys, 1, 2, txn4))

	txn5 := "330"
	txn5ReadKeys := []string{"acc31", "acc8"}
	txn5WriteKeys := []string{"acc0", "acc69"}
	assert.True(t, scheduler.ProcessTxn(txn5ReadKeys, txn5WriteKeys, 1, 2, txn5))

	txn6 := "df7"
	txn6ReadKeys := []string{"acc77", "acc83"}
	txn6WriteKeys := []string{"acc0", "acc98"}
	assert.True(t, scheduler.ProcessTxn(txn6ReadKeys, txn6WriteKeys, 1, 2, txn6))

	txn7 := "8f1"
	txn7ReadKeys := []string{"acc46", "acc93"}
	txn7WriteKeys := []string{"acc0", "acc48"}
	assert.True(t, scheduler.ProcessTxn(txn7ReadKeys, txn7WriteKeys, 1, 2, txn7))

	txn8 := "1e2"
	txn8ReadKeys := []string{"acc50", "acc87"}
	txn8WriteKeys := []string{"acc19", "acc69"}
	assert.True(t, scheduler.ProcessTxn(txn8ReadKeys, txn8WriteKeys, 1, 2, txn8))

	txn9 := "9c0"
	txn9ReadKeys := []string{"acc37", "acc98"}
	txn9WriteKeys := []string{"acc0", "acc93"}
	assert.True(t, scheduler.ProcessTxn(txn9ReadKeys, txn9WriteKeys, 1, 2, txn9))

	txn10 := "acc"
	txn10ReadKeys := []string{"acc83", "acc97"}
	txn10WriteKeys := []string{"acc46", "acc6"}
	assert.True(t, scheduler.ProcessTxn(txn10ReadKeys, txn10WriteKeys, 1, 2, txn10))

	fmt.Println("++++++++++++++++++++Retrieve Schedule++++++++++++")
	_ = scheduler.ProcessBlk(blkHeight)
	// assert.Equal(t, map[string]bool{txn3: true, txn2: true, txn1: true}, ToMap(scheduler.ProcessBlk(blkHeight)))
	fmt.Println("^^^^^^^^^^^^^^^^^Report Detail Stats^^^^^^^^^^^^^^^")
	scheduler.detailedStat(blkHeight)
}

func TestScheduler3(t *testing.T) {
	path := "/tmp/ts3"
	os.RemoveAll(path)
	scheduler := createTxnScheduler(5, true, path)
	fmt.Println("===========================Start Block 1==========================")
	blkHeight := uint64(1)
	txn0ReadKeys := []string{}
	txn0WriteKeys := []string{}
	for i := 0; i < 100; i++ {
		txn0WriteKeys = append(txn0WriteKeys, "acc"+strconv.Itoa(i))
	}

	txn0 := "4c2"
	fmt.Println("---Process Txn " + txn0)
	assert.True(t, scheduler.ProcessTxn(txn0ReadKeys, txn0WriteKeys, 1, 1, txn0))
	fmt.Println("++++++++++++++++++++Retrieve Schedule++++++++++++")
	assert.Equal(t, []string{txn0}, scheduler.ProcessBlk(blkHeight))
	fmt.Println("^^^^^^^^^^^^^^^^^Report Detail Stats^^^^^^^^^^^^^^^")
	scheduler.detailedStat(blkHeight)

	var txn string
	var readKeys []string
	var writeKeys []string

	blkHeight = 2
	fmt.Printf("\n\n\n\n===========================Start Block blkHeight %d============================\n", blkHeight)
	txn = "7c6"
	readKeys = []string{"acc28", "acc8"}
	writeKeys = []string{"acc0", "acc54"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 1, blkHeight, txn))

	txn = "ab4"
	readKeys = []string{"acc37", "acc71"}
	writeKeys = []string{"acc0", "acc3"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 1, blkHeight, txn))

	txn = "e86"
	readKeys = []string{"acc21", "acc45"}
	writeKeys = []string{"acc90", "acc93"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 1, blkHeight, txn))

	txn = "b43"
	readKeys = []string{"acc0", "acc61"}
	writeKeys = []string{"acc0", "acc7"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 1, blkHeight, txn))

	txn = "ead"
	readKeys = []string{"acc31"}
	writeKeys = []string{"acc16", "acc79"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 1, blkHeight, txn))

	fmt.Println("++++++++++++++++++++Retrieve Schedule++++++++++++")
	_ = scheduler.ProcessBlk(blkHeight)
	fmt.Println("^^^^^^^^^^^^^^^^^Report Detail Stats^^^^^^^^^^^^^^^")
	scheduler.detailedStat(blkHeight)

	blkHeight = 3
	fmt.Printf("\n\n\n\n===========================Start Block blkHeight %d============================\n", blkHeight)
	txn = "e2e"
	readKeys = []string{"acc35", "acc41"}
	writeKeys = []string{"acc0", "acc45"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 1, blkHeight, txn))

	txn = "fbd"
	readKeys = []string{"acc6", "acc7"}
	writeKeys = []string{"acc63", "acc8"}
	assert.False(t, scheduler.ProcessTxn(readKeys, writeKeys, 1, blkHeight, txn))

	txn = "001"
	readKeys = []string{"acc52", "acc76"}
	writeKeys = []string{"acc71", "acc85"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 1, blkHeight, txn))

	txn = "69c"
	readKeys = []string{"acc23", "acc25"}
	writeKeys = []string{"acc7", "acc91"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 2, blkHeight, txn))

	txn = "ac9"
	readKeys = []string{"acc30", "acc52"}
	writeKeys = []string{"acc32", "acc89"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 1, blkHeight, txn))

	txn = "207"
	readKeys = []string{"acc64", "acc98"}
	writeKeys = []string{"acc0", "acc34"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 1, blkHeight, txn))

	fmt.Println("++++++++++++++++++++Retrieve Schedule++++++++++++")
	_ = scheduler.ProcessBlk(blkHeight)
	fmt.Println("^^^^^^^^^^^^^^^^^Report Detail Stats^^^^^^^^^^^^^^^")
	scheduler.detailedStat(blkHeight)

	blkHeight = 4
	fmt.Printf("\n\n\n\n===========================Start Block blkHeight %d============================\n", blkHeight)
	txn = "3f3"
	readKeys = []string{"acc24", "acc78"}
	writeKeys = []string{"acc0", "acc83"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 1, blkHeight, txn))

	// Cycle detected
	txn = "700"
	readKeys = []string{"acc45", "acc77"}
	writeKeys = []string{"acc0", "acc68"}
	assert.False(t, scheduler.ProcessTxn(readKeys, writeKeys, 2, blkHeight, txn))

	txn = "a79"
	readKeys = []string{"acc28", "acc86"}
	writeKeys = []string{"acc42", "acc62"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 2, blkHeight, txn))

	// Cycle detected
	txn = "ff3"
	readKeys = []string{"acc0", "acc15"}
	writeKeys = []string{"acc0", "acc37"}
	assert.False(t, scheduler.ProcessTxn(readKeys, writeKeys, 2, blkHeight, txn))

	txn = "b19"
	readKeys = []string{"acc22", "acc76"}
	writeKeys = []string{"acc36", "acc38"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 2, blkHeight, txn))

	txn = "9ac"
	readKeys = []string{"acc28", "acc64"}
	writeKeys = []string{"acc0"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 3, blkHeight, txn))

	txn = "7c3"
	readKeys = []string{"acc27", "acc6"}
	writeKeys = []string{"acc60", "acc62"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 3, blkHeight, txn))

	fmt.Println("++++++++++++++++++++Retrieve Schedule++++++++++++")
	_ = scheduler.ProcessBlk(blkHeight)
	fmt.Println("^^^^^^^^^^^^^^^^^Report Detail Stats^^^^^^^^^^^^^^^")
	scheduler.detailedStat(blkHeight)

	blkHeight = 5
	fmt.Printf("\n\n\n\n===========================Start Block blkHeight %d============================\n", blkHeight)
	txn = "11e"
	readKeys = []string{"acc0", "acc5"}
	writeKeys = []string{"acc61", "acc97"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 3, blkHeight, txn))

	txn = "4d4"
	readKeys = []string{"acc45", "acc72"}
	writeKeys = []string{"acc29", "acc3"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 3, blkHeight, txn))

	txn = "ea5"
	readKeys = []string{"acc24", "acc82"}
	writeKeys = []string{"acc16", "acc56"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 3, blkHeight, txn))

	//Cycle
	txn = "380"
	readKeys = []string{"acc0", "acc56"}
	writeKeys = []string{"acc0", "acc22"}
	assert.False(t, scheduler.ProcessTxn(readKeys, writeKeys, 3, blkHeight, txn))

	// Cycle
	txn = "d26"
	readKeys = []string{"acc38", "acc46"}
	writeKeys = []string{"acc35", "acc36"}
	assert.False(t, scheduler.ProcessTxn(readKeys, writeKeys, 3, blkHeight, txn))

	txn = "846"
	readKeys = []string{"acc39", "acc7"}
	writeKeys = []string{"acc0", "acc98"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 3, blkHeight, txn))

	txn = "d77"
	readKeys = []string{"acc0", "acc48"}
	writeKeys = []string{"acc34", "acc43"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 4, blkHeight, txn))

	fmt.Println("++++++++++++++++++++Retrieve Schedule++++++++++++")
	_ = scheduler.ProcessBlk(blkHeight)
	fmt.Println("^^^^^^^^^^^^^^^^^Report Detail Stats^^^^^^^^^^^^^^^")
	scheduler.detailedStat(blkHeight)

	blkHeight = 6
	fmt.Printf("\n\n\n\n===========================Start Block blkHeight %d============================\n", blkHeight)
	txn = "1f8"
	readKeys = []string{"acc66", "acc95"}
	writeKeys = []string{"acc3", "acc34"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 4, blkHeight, txn))

	txn = "347"
	readKeys = []string{"acc50", "acc62"}
	writeKeys = []string{"acc8", "acc84"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 4, blkHeight, txn))

	txn = "34e"
	readKeys = []string{"acc3", "acc53"}
	writeKeys = []string{"acc63", "acc81"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 4, blkHeight, txn))

	txn = "c49"
	readKeys = []string{"acc50", "acc81"}
	writeKeys = []string{"acc0", "acc52"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 5, blkHeight, txn))

	txn = "398"
	readKeys = []string{"acc16", "acc33"}
	writeKeys = []string{"acc72", "acc88"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 4, blkHeight, txn))

	fmt.Println("++++++++++++++++++++Retrieve Schedule++++++++++++")
	_ = scheduler.ProcessBlk(blkHeight)
	fmt.Println("^^^^^^^^^^^^^^^^^Report Detail Stats^^^^^^^^^^^^^^^")
	scheduler.detailedStat(blkHeight)

	blkHeight = 7
	fmt.Printf("\n\n\n\n===========================Start Block blkHeight %d============================\n", blkHeight)
	txn = "af5"
	readKeys = []string{"acc3", "acc69"}
	writeKeys = []string{"acc0", "acc32"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 4, blkHeight, txn))

	txn = "168"
	readKeys = []string{"acc33", "acc87"}
	writeKeys = []string{"acc19", "acc97"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 5, blkHeight, txn))

	txn = "e26"
	readKeys = []string{"acc36", "acc9"}
	writeKeys = []string{"acc74", "acc92"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 5, blkHeight, txn))

	txn = "7b2"
	readKeys = []string{"acc65", "acc82"}
	writeKeys = []string{"acc23", "acc75"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 5, blkHeight, txn))

	txn = "e92"
	readKeys = []string{"acc13", "acc9"}
	writeKeys = []string{"acc14", "acc20"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 5, blkHeight, txn))

	fmt.Println("++++++++++++++++++++Retrieve Schedule++++++++++++")
	_ = scheduler.ProcessBlk(blkHeight)
	fmt.Println("^^^^^^^^^^^^^^^^^Report Detail Stats^^^^^^^^^^^^^^^")
	scheduler.detailedStat(blkHeight)

	blkHeight = 8
	fmt.Printf("\n\n\n\n===========================Start Block blkHeight %d============================\n", blkHeight)
	txn = "57a"
	readKeys = []string{"acc31", "acc36"}
	writeKeys = []string{"acc0", "acc33"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 5, blkHeight, txn))

	txn = "256"
	readKeys = []string{"acc59", "acc96"}
	writeKeys = []string{"acc0", "acc37"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 6, blkHeight, txn))
}

func TestScheduler4(t *testing.T) {
	path := "/tmp/ts3"
	os.RemoveAll(path)
	scheduler := createTxnScheduler(5, true, path)
	fmt.Println("===========================Start Block 1==========================")
	blkHeight := uint64(1)
	txn0ReadKeys := []string{}
	//TODO:
	txn0WriteKeys := []string{}
	for i := 0; i < 100; i++ {
		txn0WriteKeys = append(txn0WriteKeys, "acc"+strconv.Itoa(i))
	}

	txn0 := "529"
	fmt.Println("---Process Txn " + txn0)
	assert.True(t, scheduler.ProcessTxn(txn0ReadKeys, txn0WriteKeys, 1, 1, txn0))
	fmt.Println("++++++++++++++++++++Retrieve Schedule++++++++++++")
	assert.Equal(t, []string{txn0}, scheduler.ProcessBlk(blkHeight))
	fmt.Println("^^^^^^^^^^^^^^^^^Report Detail Stats^^^^^^^^^^^^^^^")
	scheduler.detailedStat(blkHeight)

	var txn string
	var readKeys []string
	var writeKeys []string

	blkHeight = 2
	fmt.Printf("\n\n\n\n===========================Start Block blkHeight %d============================\n", blkHeight)
	txn = "e2b"
	readKeys = []string{"acc45", "acc92"}
	writeKeys = []string{"acc39", "acc79"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 1, blkHeight, txn))

	txn = "df3"
	readKeys = []string{"acc27", "acc8"}
	writeKeys = []string{"acc70", "acc95"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 1, blkHeight, txn))

	txn = "bde"
	readKeys = []string{"acc91", "acc8"}
	writeKeys = []string{"acc0", "acc89"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 1, blkHeight, txn))

	txn = "396"
	readKeys = []string{"acc83", "acc91"}
	writeKeys = []string{"acc14", "acc52"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 1, blkHeight, txn))

	txn = "55d"
	readKeys = []string{"acc61", "acc85"}
	writeKeys = []string{"acc38", "acc42"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 1, blkHeight, txn))

	fmt.Println("++++++++++++++++++++Retrieve Schedule++++++++++++")
	_ = scheduler.ProcessBlk(blkHeight)
	fmt.Println("^^^^^^^^^^^^^^^^^Report Detail Stats^^^^^^^^^^^^^^^")
	scheduler.detailedStat(blkHeight)

	blkHeight = 3
	fmt.Printf("\n\n\n\n===========================Start Block blkHeight %d============================\n", blkHeight)

	txn = "136"
	readKeys = []string{"acc18", "acc8"}
	writeKeys = []string{"acc0"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 1, blkHeight, txn))

	txn = "36f"
	readKeys = []string{"acc0", "acc57"}
	writeKeys = []string{"acc31", "acc42"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 1, blkHeight, txn))

	txn = "a94"
	readKeys = []string{"acc0", "acc85"}
	writeKeys = []string{"acc28", "acc41"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 1, blkHeight, txn))

	txn = "c78"
	readKeys = []string{"acc64", "acc73"}
	writeKeys = []string{"acc29", "acc82"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 2, blkHeight, txn))

	txn = "1c1"
	readKeys = []string{"acc18", "acc62"}
	writeKeys = []string{"acc0", "acc56"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 2, blkHeight, txn))

	fmt.Println("++++++++++++++++++++Retrieve Schedule++++++++++++")
	_ = scheduler.ProcessBlk(blkHeight)
	fmt.Println("^^^^^^^^^^^^^^^^^Report Detail Stats^^^^^^^^^^^^^^^")
	scheduler.detailedStat(blkHeight)

	blkHeight = 4
	fmt.Printf("\n\n\n\n===========================Start Block blkHeight %d============================\n", blkHeight)

	txn = "422"
	readKeys = []string{"acc39", "acc40"}
	writeKeys = []string{"acc0", "acc13"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 2, blkHeight, txn))

	txn = "465"
	readKeys = []string{"acc32", "acc41"}
	writeKeys = []string{"acc0", "acc47"}
	assert.False(t, scheduler.ProcessTxn(readKeys, writeKeys, 2, blkHeight, txn))

	txn = "295"
	readKeys = []string{"acc49", "acc83"}
	writeKeys = []string{"acc13", "acc8"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 3, blkHeight, txn))

	txn = "a68"
	readKeys = []string{"acc25", "acc35"}
	writeKeys = []string{"acc20", "acc96"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 3, blkHeight, txn))

	txn = "f22"
	readKeys = []string{"acc30", "acc70"}
	writeKeys = []string{"acc30", "acc77"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 3, blkHeight, txn))

	txn = "830"
	readKeys = []string{"acc40", "acc78"}
	writeKeys = []string{"acc6", "acc93"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 3, blkHeight, txn))

	fmt.Println("++++++++++++++++++++Retrieve Schedule++++++++++++")
	_ = scheduler.ProcessBlk(blkHeight)
	fmt.Println("^^^^^^^^^^^^^^^^^Report Detail Stats^^^^^^^^^^^^^^^")
	scheduler.detailedStat(blkHeight)

	blkHeight = 5
	fmt.Printf("\n\n\n\n===========================Start Block blkHeight %d============================\n", blkHeight)
	txn = "cc9"
	readKeys = []string{"acc0", "acc4"}
	writeKeys = []string{"acc0"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 4, blkHeight, txn))

	txn = "b30"
	readKeys = []string{"acc68", "acc94"}
	writeKeys = []string{"acc0", "acc58"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 4, blkHeight, txn))

	txn = "688"
	readKeys = []string{"acc29", "acc64"}
	writeKeys = []string{"acc0", "acc19"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 4, blkHeight, txn))

	txn = "760"
	readKeys = []string{"acc13", "acc77"}
	writeKeys = []string{"acc0", "acc52"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 4, blkHeight, txn))

	txn = "62c"
	readKeys = []string{"acc3", "acc61"}
	writeKeys = []string{"acc39", "acc9"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 4, blkHeight, txn))

	fmt.Println("++++++++++++++++++++Retrieve Schedule++++++++++++")
	_ = scheduler.ProcessBlk(blkHeight)
	fmt.Println("^^^^^^^^^^^^^^^^^Report Detail Stats^^^^^^^^^^^^^^^")
	scheduler.detailedStat(blkHeight)

	blkHeight = 6
	fmt.Printf("\n\n\n\n===========================Start Block blkHeight %d============================\n", blkHeight)
	txn = "68f"
	readKeys = []string{"acc31", "acc60"}
	writeKeys = []string{"acc0"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 4, blkHeight, txn))

	txn = "89b"
	readKeys = []string{"acc0", "acc79"}
	writeKeys = []string{"acc41", "acc89"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 4, blkHeight, txn))

	txn = "63b"
	readKeys = []string{"acc0", "acc35"}
	writeKeys = []string{"acc0", "acc90"}
	assert.False(t, scheduler.ProcessTxn(readKeys, writeKeys, 4, blkHeight, txn))

	txn = "405"
	readKeys = []string{"acc19", "acc67"}
	writeKeys = []string{"acc0", "acc93"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 5, blkHeight, txn))

	txn = "f3f"
	readKeys = []string{"acc31", "acc55"}
	writeKeys = []string{"acc36", "acc78"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 5, blkHeight, txn))

	fmt.Println("++++++++++++++++++++Retrieve Schedule++++++++++++")
	_ = scheduler.ProcessBlk(blkHeight)
	fmt.Println("^^^^^^^^^^^^^^^^^Report Detail Stats^^^^^^^^^^^^^^^")
	scheduler.detailedStat(blkHeight)

	blkHeight = 7
	fmt.Printf("\n\n\n\n===========================Start Block blkHeight %d============================\n", blkHeight)
	txn = "d77"
	readKeys = []string{"acc59", "acc62"}
	writeKeys = []string{"acc0", "acc52"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 5, blkHeight, txn))

	txn = "9a4"
	readKeys = []string{"acc0", "acc12"}
	writeKeys = []string{"acc42", "acc47"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 6, blkHeight, txn))

	txn = "671"
	readKeys = []string{"acc15", "acc87"}
	writeKeys = []string{"acc0", "acc70"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 6, blkHeight, txn))

	txn = "ae8"
	readKeys = []string{"acc51", "acc70"}
	writeKeys = []string{"acc37", "acc85"}
	assert.True(t, scheduler.ProcessTxn(readKeys, writeKeys, 6, blkHeight, txn))

}
