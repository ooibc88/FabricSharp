package main

import (
	"crypto/sha512"
	"encoding/hex"
	"encoding/json"
	"strconv"
	"strings"

	"github.com/hyperledger/fabric-chaincode-go/shim"

	pb "github.com/hyperledger/fabric-protos-go/peer"
)

const ERROR_UNKNOWN_FUNC = "Unknown function"
const ERROR_WRONG_ARGS = "Wrong arguments of function"
const ERROR_SYSTEM = "System exception"
const ERR_NOT_FOUND = "Could not find specified account"
const ERROR_PUT_STATE = "Failed to put state"

var namespace = hexdigest("smallbank")[:6]

type SmallbankChaincode struct {
}

func (t *SmallbankChaincode) Init(stub shim.ChaincodeStubInterface) pb.Response {
	// nothing to do
	return shim.Success(nil)
}

func (t *SmallbankChaincode) Invoke(stub shim.ChaincodeStubInterface) pb.Response {
	function, args := stub.GetFunctionAndParameters()
	switch function {
	case "create_account":
		return t.CreateAccount(stub, args)
	case "transact_savings":
		return t.TransactSavings(stub, args)
	case "deposit_checking":
		return t.DepositChecking(stub, args)
	case "send_payment":
		return t.SendPayment(stub, args)
	case "write_check":
		return t.WriteCheck(stub, args)
	case "amalgamate":
		return t.Amalgamate(stub, args)
	case "query":
		return t.Query(stub, args)
	default:
		return errormsg(ERROR_UNKNOWN_FUNC + ": " + function)
	}
}

type Account struct {
	CustomId        string
	CustomName      string
	SavingsBalance  int
	CheckingBalance int
}

func (t *SmallbankChaincode) CreateAccount(stub shim.ChaincodeStubInterface,
	args []string) pb.Response {
	if len(args) != 4 {
		return errormsg(ERROR_WRONG_ARGS + " create_account")
	}

	checking, errcheck := strconv.Atoi(args[2])
	if errcheck != nil {
		return errormsg(ERROR_WRONG_ARGS)
	}
	saving, errsaving := strconv.Atoi(args[3])
	if errsaving != nil {
		return errormsg(ERROR_WRONG_ARGS)
	}

	account := &Account{
		CustomId:        args[0],
		CustomName:      args[1],
		SavingsBalance:  saving,
		CheckingBalance: checking}
	err := saveAccount(stub, account)
	if err != nil {
		return systemerror(err.Error())
	}

	return shim.Success(nil)
}

func (t *SmallbankChaincode) DepositChecking(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	if len(args) != 2 {
		return errormsg(ERROR_WRONG_ARGS + " deposit_checking")
	}
	account, err := loadAccount(stub, args[1])
	if err != nil {
		return errormsg(ERR_NOT_FOUND + " " + args[1])
	}
	amount, _ := strconv.Atoi(args[0])
	account.CheckingBalance += amount
	err = saveAccount(stub, account)
	if err != nil {
		return systemerror(err.Error())
	}

	return shim.Success(nil)
}

func (t *SmallbankChaincode) WriteCheck(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	if len(args) != 2 {
		return errormsg(ERROR_WRONG_ARGS + " write_check")
	}
	account, err := loadAccount(stub, args[1])
	if err != nil {
		return errormsg(ERR_NOT_FOUND + " " + args[1])
	}
	amount, _ := strconv.Atoi(args[0])
	account.CheckingBalance -= amount
	err = saveAccount(stub, account)
	if err != nil {
		return systemerror(err.Error())
	}

	return shim.Success(nil)
}

func (t *SmallbankChaincode) TransactSavings(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	if len(args) != 2 { // should be [amount,customer_id]
		return errormsg(ERROR_WRONG_ARGS + " transaction_savings")
	}
	account, err := loadAccount(stub, args[1])
	if err != nil {
		return errormsg(ERR_NOT_FOUND + " " + args[1])
	}
	amount, _ := strconv.Atoi(args[0])

	account.SavingsBalance += amount
	err = saveAccount(stub, account)
	if err != nil {
		return systemerror(err.Error())
	}

	return shim.Success(nil)
}

func (t *SmallbankChaincode) SendPayment(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	if len(args) != 3 {
		return errormsg(ERROR_WRONG_ARGS + " send_payment")
	}
	destAccount, err1 := loadAccount(stub, args[1])
	sourceAccount, err2 := loadAccount(stub, args[2])
	if err1 != nil {
		return errormsg(ERR_NOT_FOUND + " " + args[1])
	}

	if err2 != nil {
		return errormsg(ERR_NOT_FOUND + " " + args[2])
	}

	amount, _ := strconv.Atoi(args[0])
	sourceAccount.CheckingBalance -= amount
	destAccount.CheckingBalance += amount
	err1 = saveAccount(stub, sourceAccount)
	err2 = saveAccount(stub, destAccount)
	if err1 != nil || err2 != nil {
		return errormsg(ERROR_PUT_STATE)
	}

	return shim.Success(nil)
}

func (t *SmallbankChaincode) Amalgamate(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	if len(args) != 2 {
		return errormsg(ERROR_WRONG_ARGS + " amalgamate")
	}
	destAccount, err1 := loadAccount(stub, args[0])
	sourceAccount, err2 := loadAccount(stub, args[1])
	if err1 != nil {
		return errormsg(ERR_NOT_FOUND + " " + args[0])
	}

	if err2 != nil {
		return errormsg(ERR_NOT_FOUND + " " + args[1])
	}

	destAccount.CheckingBalance += sourceAccount.SavingsBalance
	sourceAccount.SavingsBalance = 0
	err1 = saveAccount(stub, sourceAccount)
	err2 = saveAccount(stub, destAccount)
	if err1 != nil || err2 != nil {
		return errormsg(ERROR_PUT_STATE)
	}

	return shim.Success(nil)
}

func (t *SmallbankChaincode) Query(stub shim.ChaincodeStubInterface, args []string) pb.Response {
	key := accountKey(args[0])
	accountBytes, err := stub.GetState(key)
	if err != nil {
		return systemerror(err.Error())
	}

	return shim.Success(accountBytes)
}

func main() {
	_ = shim.Start(new(SmallbankChaincode))
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

func accountKey(id string) string {
	// return namespace + hexdigest(id)[:64]
	return namespace + id
}

func loadAccount(stub shim.ChaincodeStubInterface, id string) (*Account, error) {
	key := accountKey(id)
	accountBytes, err := stub.GetState(key)
	if err != nil {
		return nil, err
	}
	res := Account{}
	err = json.Unmarshal(accountBytes, &res)
	if err != nil {
		return nil, err
	}
	return &res, nil
}

func saveAccount(stub shim.ChaincodeStubInterface, account *Account) error {
	accountBytes, err := json.Marshal(account)
	if err != nil {
		return err
	}
	key := accountKey(account.CustomId)
	return stub.PutState(key, accountBytes)
}
