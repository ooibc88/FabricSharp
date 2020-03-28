package main

import (
	"fmt"
	"strconv"
	"time"

	"github.com/hyperledger/fabric/core/chaincode/shim"
	pb "github.com/hyperledger/fabric/protos/peer"
)

const ERROR_SYSTEM = "{\"code\":300, \"reason\": \"system error: %s\"}"
const ERROR_WRONG_FORMAT = "{\"code\":301, \"reason\": \"command format is wrong\"}"
const ERROR_ACCOUNT_EXISTING = "{\"code\":302, \"reason\": \"account already exists\"}"
const ERROR_ACCOUT_ABNORMAL = "{\"code\":303, \"reason\": \"abnormal account\"}"
const ERROR_MONEY_NOT_ENOUGH = "{\"code\":304, \"reason\": \"account's money is not enough\"}"
const EMPTY_VAL = "empty"

type NewChaincode struct {
}

// Initialize the accounts
func (t *NewChaincode) Init(stub shim.ChaincodeStubInterface) pb.Response {
	// nothing to do
	_, args := stub.GetFunctionAndParameters()
	if len(args) != 1 {
		return shim.Error("Incorrect number of arguments: " + strconv.Itoa(len(args)))
	}

	nacc, err := strconv.Atoi(args[0])

	if err != nil {
		return shim.Error("Expecting integer value for number of accounts")
	}

	for i := 0; i < nacc; i++ {
		acc := fmt.Sprintf("acc%d", i)
		err = stub.PutState(acc, []byte(EMPTY_VAL))
		if err != nil {
			fmt.Println("error putting state in Init")
		}
	}

	fmt.Println("Initialized accounts")

	return shim.Success(nil)
}

func (t *NewChaincode) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	function, args := stub.GetFunctionAndParameters()

	if function == "readwrite" {
		return t.ReadWrite(stub, args)
	} else if function == "modify" {
		return t.ReadModifyWrite(stub, args)
	} else if function == "empty" {
		return shim.Success(nil)
	}

	return shim.Error("Error in Invoke function. Unkown function name: " + function)
}

func (t *NewChaincode) ReadModifyWrite(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	accountName := args[0]
	if _, err := stub.GetState(accountName); err != nil {
		s := fmt.Sprintf(ERROR_SYSTEM, err.Error())
		return shim.Error(s)
	}

	if err := stub.PutState(accountName, []byte(EMPTY_VAL)); err != nil {
		s := fmt.Sprintf(ERROR_SYSTEM, err.Error())
		return shim.Error(s)
	}
	return shim.Success(nil)
}

// [ReadWrite 0 4 acc1 acc2 acc3 acc4 acc5 2 acc5 acc6]
func (t *NewChaincode) ReadWrite(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	var readCount, writeCount int
	var err error
	var sleepIntervalMS int
	var valByte []byte
	ind := 0
	if sleepIntervalMS, err = strconv.Atoi(args[ind]); err != nil {
		return shim.Error("Format error in sleepIntervalMS")
	}

	ind++
	if readCount, err = strconv.Atoi(args[ind]); err != nil {
		return shim.Error("Format error in read count")
	}
	ind++
	for i := 0; i < readCount; i++ {
		valByte, err = stub.GetState(args[ind])
		if i < readCount-1 && sleepIntervalMS > 0 {
			time.Sleep(time.Duration(sleepIntervalMS) * time.Millisecond)
		}
		if err != nil {
			s := fmt.Sprintf(ERROR_SYSTEM, err.Error())
			return shim.Error(s)
		}
		if valByte == nil {
			return shim.Error("ERROR_ACCOUNT_ABNORMAL" + args[ind])
		}
		fmt.Printf("Read key %s with value %s \n", args[ind], string(valByte))
		ind++
	}
	if writeCount, err = strconv.Atoi(args[ind]); err != nil {
		return shim.Error("Format err in write count")
	}
	ind++
	for i := 0; i < writeCount; i++ {
		if err = stub.PutState(args[ind], []byte(EMPTY_VAL)); err != nil {
			s := fmt.Sprintf(ERROR_SYSTEM, err.Error())
			return shim.Error(s)
		}
		fmt.Printf("Put key %s with value %s \n", args[ind], string(EMPTY_VAL))
		ind++
	}
	return shim.Success(nil)
}

func main() {
	err := shim.Start(new(NewChaincode))
	if err != nil {
		fmt.Printf("Error starting chaincode: %v \n", err)
	}

}
