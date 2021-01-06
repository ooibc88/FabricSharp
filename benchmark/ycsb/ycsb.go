package main

import (
	"crypto/sha512"
	"encoding/hex"
	"strconv"
	"strings"

	"github.com/hyperledger/fabric-chaincode-go/shim"
	pb "github.com/hyperledger/fabric-protos-go/peer"
)

const ERROR_UNKNOWN_FUNC = "Unknown function"
const ERROR_WRONG_ARGS = "Wrong arguments of function"
const ERROR_SYSTEM = "System exception"
const ERR_NOT_FOUND = "Could not find specified Key"
const ERROR_PUT_STATE = "Failed to put state"

var namespace = hexdigest("ycsb")[:6]

type YCSBChaincode struct {
}

func (t *YCSBChaincode) Init(stub shim.ChaincodeStubInterface) pb.Response {
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
	case "multirw":
		return t.MultiRW(stub, args)
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
	if len(args) != 2 {
		return errormsg(ERROR_WRONG_ARGS + " Insert")
	}

	err := stub.PutState(args[0], []byte(args[1]))
	if err != nil {
		return systemerror(err.Error())
	}

	return shim.Success(nil)
}

func (t *YCSBChaincode) Update(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	if len(args) != 2 {
		return errormsg(ERROR_WRONG_ARGS + " Update")
	}

	err := stub.PutState(args[0], []byte(args[1]))
	if err != nil {
		return systemerror(err.Error())
	}

	return shim.Success(nil)
}

func (t *YCSBChaincode) ReadModifyWrite(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	if len(args) != 2 {
		return errormsg(ERROR_WRONG_ARGS + " ReadModifyWrite")
	}

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
	if err != nil {
		return systemerror(err.Error())
	}

	return shim.Success(valBytes)
}

func (t *YCSBChaincode) MultiRW(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	arg_count, _ := strconv.Atoi(args[0])
	for i := 1; i <= arg_count; i++ {
		valBytes, _ := stub.GetState(args[i])
		_ = stub.PutState(args[i], valBytes)
	}
	return shim.Success(nil)
}

func main() {
	_ = shim.Start(new(YCSBChaincode))
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
