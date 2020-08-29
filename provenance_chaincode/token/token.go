package main

import (
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/RUAN0007/fabric-chaincode-go/shim"
	pb "github.com/hyperledger/fabric-protos-go/peer"
)

const ERROR_UNKNOWN_FUNC = "Unknown function"
const ERROR_SYSTEM = "System exception"
const ERROR_WRONG_ARGS = "Wrong arguments of function"

type TokenChaincode struct {
}

const BLACKLISTED_KEY = "blacklisted"

func (t *TokenChaincode) Init(stub shim.ChaincodeStubInterface) pb.Response {
	// Put the initial blacklisted address
	if err := stub.PutState(BLACKLISTED_KEY, []byte("AccountA_AccountB_AccountC")); err != nil {
		return systemerror(err.Error())
	}
	return shim.Success(nil)
}

func (t *TokenChaincode) Prov(stub shim.ChaincodeStubInterface, reads, writes map[string][]byte) map[string][]string {
	dependency := make(map[string][]string)
	function, args := stub.GetFunctionAndParameters()
	if function == "transfer" {
		// Unlike shown in the paper, here we take the shortcut by directly infer
		// the sender and receipient account from the transaction argument and return the dependency
		senderAccount := args[0]
		recipientAccount := args[1]
		dependency[recipientAccount] = []string{senderAccount}
	} else {
		// other functions  do not involve any provenance-related relationship.
		// Hence, do nothing.
	}
	return dependency
}

func (t *TokenChaincode) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	function, args := stub.GetFunctionAndParameters()
	switch function {
	case "open_account":
		return t.OpenAccount(stub, args)
	case "transfer":
		return t.Transfer(stub, args)
	case "refund":
		return t.Refund(stub, args)
	case "blacklist":
		return t.Blacklist(stub, args)
	case "query":
		return t.Query(stub, args)
	default:
		return errormsg(ERROR_UNKNOWN_FUNC + ": " + function)
	}
}

func (t *TokenChaincode) Refund(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	if len(args) != 1 { //should be [accountName]
		return errormsg(ERROR_WRONG_ARGS + " Refund")
	}
	totalBalance := 0
	count := 0
	account := args[0]
	// Assume the height of the first block in the current month is 10
	var thresholdBlkHeight uint64 = 10
	var curBlkHeight uint64 = math.MaxUint64
	for thresholdBlkHeight < curBlkHeight {
		if balanceBytes, committedBlkHeight, err := stub.Hist(account, curBlkHeight); err != nil {
			return systemerror(err.Error())
		} else if balance, err := strconv.Atoi(string(balanceBytes)); err != nil {
			return systemerror(err.Error())
		} else {
			totalBalance += balance
			count++
			curBlkHeight = committedBlkHeight - 1
		}
	}
	refundAmount := totalBalance / count
	newBalance := ""
	if balanceBytes, err := stub.GetState(account); err != nil {
		return systemerror(err.Error())
	} else if balance, err := strconv.Atoi(string(balanceBytes)); err != nil {
		return systemerror(err.Error())
	} else {
		newBalance = strconv.Itoa(balance + refundAmount)
	}

	if err := stub.PutState(account, []byte(newBalance)); err != nil {
		return systemerror(err.Error())
	}
	return shim.Success(nil)
}

func (t *TokenChaincode) getBlacklist(stub shim.ChaincodeStubInterface) ([]string, error) {
	if valueBytes, err := stub.GetState(BLACKLISTED_KEY); err != nil {
		return nil, err
	} else {
		value := string(valueBytes)
		accounts := strings.Split(value, "_")
		return accounts, nil
	}
}

func (t *TokenChaincode) addToBlacklist(stub shim.ChaincodeStubInterface, account string) pb.Response {
	if valueBytes, err := stub.GetState(BLACKLISTED_KEY); err != nil {
		return shim.Success(nil)
	} else {
		newValue := string(valueBytes) + "_" + account
		if err := stub.PutState(BLACKLISTED_KEY, []byte(newValue)); err != nil {
			return systemerror(err.Error())
		} else {
			return shim.Success(nil)
		}
	}
}

func (t *TokenChaincode) Blacklist(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	account := args[0]
	if blackListed, err := t.getBlacklist(stub); err != nil {
		return systemerror(err.Error())
	} else {
		var lastBlk uint64 = math.MaxUint64
		blackListedMap := make(map[string]bool)
		for _, b := range blackListed {
			blackListedMap[b] = true
		}
		for i := 0; i < 5; i++ {
			if _, committedBlk, err := stub.Hist(account, lastBlk); err != nil {
				return shim.Success(nil) // return nil as the total number of balance version is below 5.
			} else {
				if depKeys, _, _, err := stub.Backward(account, committedBlk); err != nil {
					return errormsg(err.Error())
				} else {
					for _, depKey := range depKeys {
						if _, ok := blackListedMap[depKey]; ok {
							return t.addToBlacklist(stub, account)
						}
					}
				}

				if antiDepKeys, _, _, err := stub.Forward(account, committedBlk); err != nil {
					return errormsg(err.Error())
				} else {
					for _, antiDepKey := range antiDepKeys {
						if _, ok := blackListedMap[antiDepKey]; ok {
							return t.addToBlacklist(stub, account)
						}
					}
				}

				lastBlk = committedBlk - 1
			}
		}
	}
	return shim.Success(nil)
}

func (t *TokenChaincode) OpenAccount(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	if len(args) != 2 { //should be [accountName, initial_balance]
		return errormsg(ERROR_WRONG_ARGS + " Open Account")
	}
	accountName := args[0]
	if data, err := stub.GetState(accountName); err != nil {
		return errormsg(err.Error())
	} else if data != nil {
		return errormsg("Can not create duplicated account")
	}

	if _, err := strconv.Atoi(args[1]); err != nil {
		return systemerror(err.Error())
	}

	if err := stub.PutState(accountName, []byte(args[1])); err != nil {
		return systemerror(err.Error())
	}
	return shim.Success(nil)
}

func (t *TokenChaincode) Query(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	key := args[0]
	balanceBytes, err := stub.GetState(key)
	if err != nil {
		return systemerror(err.Error())
	}

	return shim.Success(balanceBytes)
}

func (t *TokenChaincode) Transfer(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	if len(args) != 3 { //should be [senderAccount, recipientAccount, transferAmount]
		return errormsg(ERROR_WRONG_ARGS + " Open Account")
	}
	senderAccount := args[0]
	recipientAccount := args[1]
	transferAmount := 0
	if parsed, err := strconv.Atoi(args[2]); err != nil {
		return systemerror(err.Error())
	} else {
		transferAmount = parsed
	}

	var senderNewBalance, recipientNewBalance string
	if senderBalanceBytes, err := stub.GetState(senderAccount); err != nil {
		return systemerror(err.Error())
	} else if senderBalance, err := strconv.Atoi(string(senderBalanceBytes)); err != nil {
		return systemerror(err.Error())
	} else if senderBalance < transferAmount {
		return errormsg("Insufficient fund in Account " + senderAccount)
	} else {
		senderNewBalance = strconv.Itoa(senderBalance - transferAmount)
	}

	if recipientBalanceBytes, err := stub.GetState(recipientAccount); err != nil {
		return systemerror(err.Error())
	} else if receipientBalance, err := strconv.Atoi(string(recipientBalanceBytes)); err != nil {
		return systemerror(err.Error())
	} else {
		recipientNewBalance = strconv.Itoa(receipientBalance + transferAmount)
	}

	if err := stub.PutState(senderAccount, []byte(senderNewBalance)); err != nil {
		return systemerror(err.Error())
	}

	if err := stub.PutState(recipientAccount, []byte(recipientNewBalance)); err != nil {
		return systemerror(err.Error())
	}

	return shim.Success(nil)
}

func main() {
	err := shim.Start(new(TokenChaincode))
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
