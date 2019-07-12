package localconfig

import (
	"os"
	"strconv"
)

const (
	FoccLatest = iota
	FoccSharp
	FoccStandard
)

type SchedulerType int

func MustGetSchedulerType() SchedulerType {
	if schedulerType := os.Getenv("SCHEDULER_TYPE"); schedulerType == "latest" {
		return FoccLatest
	} else if schedulerType == "standard" {
		return FoccStandard
	} else if schedulerType == "sharp" {
		return FoccSharp
	} else {
		panic("Unrecognized Scheduler type " + schedulerType)
	}
}

func MustGetStoragePath() string {
	if storagePath, ok := os.LookupEnv("STORE_PATH"); ok {
		return storagePath
	} else {
		panic("STORE_PATH not set ")
	}
}

// GetFSharpTxnSpanLimitWithDefault is for focc-sharp only. Default is 10
func GetFSharpTxnSpanLimitWithDefault() uint64 {
	if limit, err := strconv.Atoi(os.Getenv("TXN_SPAN_LIMIT")); err != nil || limit <= 0 {
		return 10
	} else {
		return uint64(limit)
	}
}

func TryGetBlockSize() int {
	if blockSize, err := strconv.Atoi(os.Getenv("BLOCK_SIZE")); err != nil || blockSize <= 0 {
		return -1
	} else {
		return blockSize
	}
}
