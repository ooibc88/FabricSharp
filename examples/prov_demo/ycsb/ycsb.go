/**
* Copyright 2017 HUAWEI. All Rights Reserved.
*
* SPDX-License-Identifier: Apache-2.0
*
 */

package main

import (
	"crypto/sha512"
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	pb "github.com/hyperledger/fabric/protos/peer"
)

const ERROR_UNKNOWN_FUNC = "Unknown function"
const ERROR_WRONG_ARGS = "Wrong arguments of function"
const ERROR_SYSTEM = "System exception"
const ERR_NOT_FOUND = "Could not find specified Key"
const ERROR_PUT_STATE = "Failed to put state"

var namespace = hexdigest("ycsb")[:6]

type YCSBChaincode struct {
}

func (t *YCSBChaincode) Prov(stub shim.ChaincodeStubInterface, reads, writes map[string][]byte) map[string][]string {
	// No inter-key provenance relationship in YCSB workload
	return nil
}

func (t *YCSBChaincode) Init(stub shim.ChaincodeStubInterface) pb.Response {
	// nothing to do
	return shim.Success(nil)
}

func (t *YCSBChaincode) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	function, args := stub.GetFunctionAndParameters()
	switch function {
	case "insert":
		return t.Insert(stub, args)
	case "update":
		return t.Update(stub, args)
	case "readmodifywrite":
		return t.ReadModifyWrite(stub, args)
	case "remove":
		return t.Delete(stub, args)
	case "query":
		return t.Read(stub, args)
	case "fupdate":
		return t.FibonacciUpdate(stub, args)
	case "empty":
		return t.Empty(stub, args)
	default:
		return errormsg(ERROR_UNKNOWN_FUNC + ": " + function)
	}
}

func (t *YCSBChaincode) Empty(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	return shim.Success(nil)
}

func (t *YCSBChaincode) Insert(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	if len(args) != 2 { //should be [key, concat of value]
		return errormsg(ERROR_WRONG_ARGS + " Insert")
	}

	fmt.Printf("Insert Key: %s and Value: %s\n", args[0], args[1])
	err := stub.PutState(args[0], []byte(args[1]))
	if err != nil {
		return systemerror(err.Error())
	}

	return shim.Success(nil)
}

func (t *YCSBChaincode) Update(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	if len(args) != 2 { //should be [key, concat of value]
		return errormsg(ERROR_WRONG_ARGS + " Update")
	}
	fmt.Printf("Update Key: %s and Value: %s\n", args[0], args[1])

	err := stub.PutState(args[0], []byte(args[1]))
	if err != nil {
		return systemerror(err.Error())
	}

	return shim.Success(nil)
}

func (t *YCSBChaincode) ReadModifyWrite(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	if len(args) != 2 { //should be [key, concat of value]
		return errormsg(ERROR_WRONG_ARGS + " ReadModifyWrite")
	}

	fmt.Printf("ReadModifyWrite Key: %s and Value: %s\n", args[0], args[1])

	valBytes, err := stub.GetState(args[0])
	if err != nil {
		return systemerror(err.Error())
	}

	err = stub.PutState(args[0], []byte(args[1]))
	if err != nil {
		return systemerror(err.Error())
	}

	return shim.Success(valBytes)
}

func (t *YCSBChaincode) Delete(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	if len(args) != 1 { // should be [key]
		return errormsg(ERROR_WRONG_ARGS + " Delete")
	}
	fmt.Printf("Delete Key: \n", args[0])

	err := stub.PutState(args[0], []byte(""))
	if err != nil {
		return systemerror(err.Error())
	}

	return shim.Success(nil)
}

func (t *YCSBChaincode) Read(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	if len(args) != 1 { // should be [key]
		return errormsg(ERROR_WRONG_ARGS + " Read")
	}

	valBytes, err := stub.GetState(args[0])
	// fmt.Printf("Read Key: %s with Value: %s\n", args[0], string(valBytes))
	if err != nil {
		return systemerror(err.Error())
	}

	return shim.Success(valBytes)
}

// The updated value is the sum of all versions of the same key in previous <scannedBlkCount> blocks from <startBlkHeight>
func (t *YCSBChaincode) FibonacciUpdate(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	if len(args) != 3 {
		return errormsg("Wrong # of parameter. Expecting [<key>, <start-blk-height>, <#-of-blk-scanned>].")
	}
	key := args[0]
	var err error
	var startBlkHeight, scannedBlkCount int
	if startBlkHeight, err = strconv.Atoi(args[1]); err != nil {
		return systemerror(err.Error())
	}
	if scannedBlkCount, err = strconv.Atoi(args[2]); err != nil {
		return systemerror(err.Error())
	}

	lastBlkHeight := uint64(startBlkHeight - scannedBlkCount)
	curBlk := uint64(startBlkHeight)
	sum := 0
	for lastBlkHeight <= curBlk {
		if val, committedBlk, err := stub.Hist(key, curBlk); err != nil {
			// err implies there are no earlier version. Hence just stop here and return.
			break
		} else if parsedVal, err := strconv.Atoi(val); err != nil {
			return systemerror(err.Error())
		} else {
			fmt.Printf("  Value = %d at blk %d \n", parsedVal, committedBlk)
			sum += parsedVal
			curBlk = committedBlk - 1
		}
	}
	fmt.Printf("FUpdate Key %s with value %d: \n", key, sum)
	if err = stub.PutState(key, []byte(strconv.Itoa(sum))); err != nil {
		return systemerror(err.Error())
	}

	return shim.Success(nil)
}

func main() {
	err := shim.Start(new(YCSBChaincode))
	if err != nil {
		fmt.Printf("Error starting chaincode: %v \n", err)
	}

}

func errormsg(msg string) pb.Response {
	return shim.Error("{\"error\":" + msg + "}")
}

func systemerror(err string) pb.Response {
	return errormsg(ERROR_SYSTEM + ":" + err)
}

func hexdigest(str string) string {
	hash := sha512.New()
	hash.Write([]byte(str))
	hashBytes := hash.Sum(nil)
	return strings.ToLower(hex.EncodeToString(hashBytes))
}
