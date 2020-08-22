package localconfig

import (
	"os"
	"strconv"
)

const (
	Original = iota
	Fpp
	FoccLatest
	FoccSharp
	FoccStandard
)

type CCType int // Concurrency Control techinque

func MustGetCCType() CCType {
	if schedulerType := os.Getenv("CC_TYPE"); schedulerType == "occ-latest" {
		return FoccLatest
	} else if schedulerType == "occ-standard" {
		return FoccStandard
	} else if schedulerType == "occ-sharp" {
		return FoccSharp
	} else if schedulerType == "fpp" {
		return Fpp
	} else if schedulerType == "original" || schedulerType == "" {
		return Original // default option
	} else {
		panic("Unrecognized Concurrency Control type " + schedulerType)
	}
}

func IsOCC() bool {
	ccType := MustGetCCType()
	return ccType == FoccLatest || ccType == FoccSharp || ccType == FoccStandard
}

func LineageSupported() bool {
	return true
	// if _, supported := os.LookupEnv("WithLineage"); supported {
	// 	return true
	// } else {
	// 	return false
	// }
}

func MustGetMvStoragePath() string {
	if storagePath, ok := os.LookupEnv("MV_STORE_PATH"); ok {
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
