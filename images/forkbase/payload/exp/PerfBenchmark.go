package microexp

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

type YCSBBenchmark struct {
	db             ProvDB
	keyCount       int
	operationCount int
	blkSize        int
	curBlk         uint64
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
const keyBase = "ycsb_user"
const txnBase = "txn"
const measureTimes = 10

func RandStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}

func NewYCSBBenchmark(db ProvDB, keyCount, operationCount, blkSize int) *YCSBBenchmark {
	return &YCSBBenchmark{db: db, keyCount: keyCount, operationCount: operationCount, blkSize: blkSize, curBlk: 0}
}

func (pbench *YCSBBenchmark) HistLoad() {
	emptyDepKeys := make([]string, 0)
	for i := 0; i < pbench.operationCount; i++ {
		pbench.curBlk = uint64(i / pbench.blkSize)
		key := keyBase + strconv.Itoa(i%pbench.keyCount)
		txnID := txnBase + strconv.Itoa(i)
		value := RandStringBytes(1000)
		pbench.db.Put(key, value, txnID, pbench.curBlk, emptyDepKeys)
		if i%pbench.blkSize == pbench.blkSize-1 {
			if digest, err := pbench.db.Commit(); err != nil {
				panic("Fail to commit")
			} else {
				fmt.Println("Commit on Block " + strconv.Itoa(int(pbench.curBlk)) + " with digest " + digest)
			}
		}
	}
}

func (pbench *YCSBBenchmark) MeasureHistQuery(blkDistances []uint64) {
	key := keyBase + strconv.Itoa(1) // any key is fine
	for _, blkDistance := range blkDistances {
		var totalTime int64
		totalTime = 0
		for i := 0; i < measureTimes; i++ {
			startTime := time.Now()
			pbench.db.Hist(key, pbench.curBlk-blkDistance)
			elapsedTime := time.Since(startTime).Nanoseconds() / 1000 // convert to us
			totalTime += elapsedTime
		}
		avgTime := totalTime / measureTimes
		fmt.Printf("Duration for the Historical Query with distance %d: %d us\n", blkDistance, avgTime)
	}
}

func (pbench *YCSBBenchmark) MeasureScan() {
	key := keyBase + strconv.Itoa(1) // any key is fine
	var totalTime int64
	totalTime = 0
	for i := 0; i < measureTimes; i++ {
		startTime := time.Now()
		pbench.db.Scan(key)
		elapsedTime := time.Since(startTime).Nanoseconds() / 1000 // convert to us
		totalTime += elapsedTime
	}
	avgTime := totalTime / measureTimes
	fmt.Printf("Duration for the Version Scan: %d us\n", avgTime)
}

const txnID = "txnID"

type ProvBenchmark struct {
	db         ProvDB
	blkSize    uint64
	curBlk     uint64
	curBlkSize uint64
}

func NewProvBenchmark(db ProvDB, blkSize uint64) *ProvBenchmark {
	return &ProvBenchmark{db: db, blkSize: blkSize, curBlk: 0, curBlkSize: 0}
}

func (pbench *ProvBenchmark) Put(key, value, txnID string, deps []string) {

	if err := pbench.db.Put(key, value, txnID, pbench.curBlk, deps); err != nil {
		panic("Fail to put for key " + key + " with error msg " + err.Error())
	}
	pbench.curBlkSize++
	if pbench.curBlkSize == pbench.blkSize {
		if digest, err := pbench.db.Commit(); err != nil {
			panic("Fail to commit blk " + strconv.Itoa(int(pbench.curBlk)))
		} else {
			fmt.Println("Commit on Block " + strconv.Itoa(int(pbench.curBlk)) + " with digest " + digest)
		}
		pbench.curBlk++
		pbench.curBlkSize = 0
	}
}

func (pbench *ProvBenchmark) SupplychainLoad() {
	baseCount := 5000
	pbench.SiliconLoad(baseCount)
	pbench.QuartzLoad(baseCount)

	pbench.MakeChip(baseCount * 10)
	pbench.MakeGlass(baseCount * 2)

	pbench.MakeCPU(0, 4*baseCount)
	pbench.MakeMemory(4*baseCount, 2*baseCount)
	pbench.MakeNand(6*baseCount, 2*baseCount)

	pbench.MakeNor(8*baseCount, 2*baseCount)
	pbench.MakeLen(4 * baseCount)

	pbench.MakeBoard(2 * baseCount)
	pbench.MakeCamera(4 * baseCount)
	pbench.MakeSSD(2 * baseCount)

	pbench.ManufacturePhone(2 * baseCount)
	pbench.Resale(2 * baseCount)
	pbench.Sell(2 * baseCount)
}

func (pbench *ProvBenchmark) SiliconLoad(count int) {
	value := RandStringBytes(1000)
	emptyDep := make([]string, 0)
	for i := 0; i < count; i++ {
		siliconKey := "Silicon" + strconv.Itoa(i)
		pbench.Put(siliconKey, value, txnID, emptyDep)
	}
}

func (pbench *ProvBenchmark) QuartzLoad(count int) {
	value := RandStringBytes(1000)
	emptyDep := make([]string, 0)
	for i := 0; i < count; i++ {
		quartzKey := "Quartz" + strconv.Itoa(i)
		pbench.Put(quartzKey, value, txnID, emptyDep)
	}
}

func (pbench *ProvBenchmark) MakeChip(count int) {
	value := RandStringBytes(1000)
	// each piece of silicon can manufacture 10 pieces of chip
	for i := 0; i < count; i++ {
		siliconKey := "Silicon" + strconv.Itoa(i/10)
		depKey := []string{siliconKey}
		chipKey := "Chip" + strconv.Itoa(i)
		pbench.Put(chipKey, value, txnID, depKey)
	}
}

func (pbench *ProvBenchmark) MakeGlass(count int) {
	value := RandStringBytes(1000)
	// each piece of quartz can manufacture 2 pieces of chip
	for i := 0; i < count; i++ {
		quartzKey := "Quartz" + strconv.Itoa(i/2)
		depKey := []string{quartzKey}
		glassKey := "Glass" + strconv.Itoa(i)
		pbench.Put(glassKey, value, txnID, depKey)
	}
}

func (pbench *ProvBenchmark) MakeCPU(startChipId, cpuCount int) {
	value := RandStringBytes(1000)
	// each piece of chip manufactures 1 cpu
	for i := 0; i < cpuCount; i++ {
		chipKey := "Chip" + strconv.Itoa(startChipId+i)
		depKey := []string{chipKey}
		cpuKey := "CPU" + strconv.Itoa(i)
		pbench.Put(cpuKey, value, txnID, depKey)
	}
}

func (pbench *ProvBenchmark) MakeMemory(startChipId, memoryCount int) {
	value := RandStringBytes(1000)
	// each piece of chip manufactures one piece of memory
	for i := 0; i < memoryCount; i++ {
		chipKey := "Chip" + strconv.Itoa(startChipId+i)
		depKey := []string{chipKey}
		memoryKey := "Memory" + strconv.Itoa(i)
		pbench.Put(memoryKey, value, txnID, depKey)
	}
}

func (pbench *ProvBenchmark) MakeNand(startChipId, nandCount int) {
	value := RandStringBytes(1000)
	// each piece of chip manufactures one piece of nand
	for i := 0; i < nandCount; i++ {
		chipKey := "Chip" + strconv.Itoa(startChipId+i)
		depKey := []string{chipKey}
		nandKey := "Nand" + strconv.Itoa(i)
		pbench.Put(nandKey, value, txnID, depKey)
	}
}

func (pbench *ProvBenchmark) MakeNor(startChipId, norCount int) {
	value := RandStringBytes(1000)
	// each piece of chip manufactures one piece of nor
	for i := 0; i < norCount; i++ {
		chipKey := "Chip" + strconv.Itoa(startChipId+i)
		depKey := []string{chipKey}
		norKey := "Nor" + strconv.Itoa(i)
		pbench.Put(norKey, value, txnID, depKey)
	}
}

func (pbench *ProvBenchmark) MakeLen(count int) {
	value := RandStringBytes(1000)
	// each piece of glass can manufacture 2 pieces of chip
	for i := 0; i < count; i++ {
		glassKey := "Glass" + strconv.Itoa(i/2)
		depKey := []string{glassKey}
		lenKey := "Len" + strconv.Itoa(i)
		pbench.Put(lenKey, value, txnID, depKey)
	}
}

func (pbench *ProvBenchmark) MakeBoard(count int) {
	value := RandStringBytes(1000)
	// 2 cpus and 1 memory manufacture one piece of board
	for i := 0; i < count; i++ {
		cpu1Key := "CPU" + strconv.Itoa(2*i)
		cpu2Key := "CPU" + strconv.Itoa(2*i+1)
		memoryKey := "Memory" + strconv.Itoa(i)
		depKey := []string{cpu1Key, cpu2Key, memoryKey}
		boardKey := "Board" + strconv.Itoa(i)
		pbench.Put(boardKey, value, txnID, depKey)
	}
}

func (pbench *ProvBenchmark) MakeSSD(count int) {
	value := RandStringBytes(1000)
	// 1 nand and 1 nor manufacture one piece of ssd
	for i := 0; i < count; i++ {
		nandKey := "Nand" + strconv.Itoa(i)
		norKey := "Nor" + strconv.Itoa(i)
		depKey := []string{nandKey, norKey}

		ssdKey := "SSD" + strconv.Itoa(i)
		pbench.Put(ssdKey, value, txnID, depKey)
	}
}

func (pbench *ProvBenchmark) MakeCamera(count int) {
	value := RandStringBytes(1000)
	// 1 len maunfactures 1 camera
	for i := 0; i < count; i++ {
		lenKey := "Len" + strconv.Itoa(i)
		depKey := []string{lenKey}

		cameraKey := "Camera" + strconv.Itoa(i)
		pbench.Put(cameraKey, value, txnID, depKey)
	}
}

func (pbench *ProvBenchmark) ManufacturePhone(count int) {
	value := RandStringBytes(1000)
	for i := 0; i < count; i++ {
		boardKey := "Board" + strconv.Itoa(i)
		ssdKey := "SSD" + strconv.Itoa(i)
		cameraKey1 := "Camera" + strconv.Itoa(2*i)
		cameraKey2 := "Camera" + strconv.Itoa(2*i+1)
		depKey := []string{boardKey, ssdKey, cameraKey1, cameraKey2}

		productKey := "Phone" + strconv.Itoa(i)
		pbench.Put(productKey, value, txnID, depKey)
	}
}

func (pbench *ProvBenchmark) Resale(count int) {
	value := RandStringBytes(1000)
	for i := 0; i < count; i++ {
		productKey := "Phone" + strconv.Itoa(i)
		depKey := []string{productKey} // self dependent

		pbench.Put(productKey, value, txnID, depKey)
	}
}

func (pbench *ProvBenchmark) Sell(count int) {
	value := RandStringBytes(1000)
	for i := 0; i < count; i++ {
		productKey := "Phone" + strconv.Itoa(i)
		depKey := []string{productKey} // self dependent

		pbench.Put(productKey, value, txnID, depKey)
	}
}

func (pbench *ProvBenchmark) BFSMeasure(level int) {
	key := "Phone" + strconv.Itoa(0)
	blkIdx := uint64(pbench.curBlk)

	startTime := time.Now()
	keys := []string{key}
	blkIdxs := []uint64{blkIdx}

	for i := 0; i < level; i++ {
		var tmpKeys []string
		var tmpBlkIdxs []uint64

		for ii, key := range keys {
			curIdx := blkIdxs[ii]
			// fmt.Printf("Key: %s blk: %d\n", key, int(curIdx))
			_, depKeys, depBlks, err := pbench.db.Backward(key, curIdx)
			if err != nil {
				panic("Encountering error " + err.Error())
			}
			for iii := range depKeys {
				_, _, err := pbench.db.Hist(depKeys[iii], depBlks[iii])
				if err == nil {
					tmpKeys = append(tmpKeys, depKeys[iii])
					tmpBlkIdxs = append(tmpBlkIdxs, depBlks[iii])
				} // end if
			} // end for
		} // end for

		keys = make([]string, len(tmpKeys))
		copy(keys, tmpKeys)

		blkIdxs = make([]uint64, len(tmpBlkIdxs))
		copy(blkIdxs, tmpBlkIdxs)
	} // end for level

	duration := time.Since(startTime).Nanoseconds() / 1000 // convert to micro-second unit
	fmt.Printf("BFS Query with Level %d Duration for  %s is %d us\n", level, key, duration)
}
