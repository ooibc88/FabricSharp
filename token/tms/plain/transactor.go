/*
Copyright IBM Corp. All Rights Reserved.

SPDX-License-Identifier: Apache-2.0
*/

package plain

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"

	"github.com/hyperledger/fabric/token/identity"

	"github.com/golang/protobuf/proto"
	"github.com/hyperledger/fabric/protos/ledger/queryresult"
	"github.com/hyperledger/fabric/protos/token"
	"github.com/hyperledger/fabric/token/ledger"
	"github.com/pkg/errors"
	"go.uber.org/zap/zapcore"
)

// A Transactor that can transfer tokens.
type Transactor struct {
	PublicCredential    []byte
	Ledger              ledger.LedgerReader
	TokenOwnerValidator identity.TokenOwnerValidator
}

// RequestTransfer creates a TokenTransaction of type transfer request
func (t *Transactor) RequestTransfer(request *token.TransferRequest) (*token.TokenTransaction, error) {
	var outputs []*token.Token

	if len(request.GetTokenIds()) == 0 {
		return nil, errors.New("no token IDs in transfer request")
	}
	if len(request.GetShares()) == 0 {
		return nil, errors.New("no shares in transfer request")
	}

	tokenType, _, err := t.getInputsFromTokenIds(request.GetTokenIds())
	if err != nil {
		return nil, err
	}

	for _, ttt := range request.GetShares() {
		err := t.TokenOwnerValidator.Validate(ttt.Recipient)
		if err != nil {
			return nil, errors.Errorf("invalid recipient in transfer request '%s'", err)
		}
		q, err := ToQuantity(ttt.Quantity)
		if err != nil {
			return nil, errors.Errorf("invalid quantity in transfer request '%s'", err)
		}

		outputs = append(outputs, &token.Token{
			Owner:    ttt.Recipient,
			Type:     tokenType,
			Quantity: q.ToUInt64(),
		})
	}

	// prepare transfer request
	transaction := &token.TokenTransaction{
		Action: &token.TokenTransaction_TokenAction{
			TokenAction: &token.TokenAction{
				Data: &token.TokenAction_Transfer{
					Transfer: &token.Transfer{
						Inputs:  request.GetTokenIds(),
						Outputs: outputs,
					},
				},
			},
		},
	}

	return transaction, nil
}

// RequestRedeem creates a TokenTransaction of type redeem request
func (t *Transactor) RequestRedeem(request *token.RedeemRequest) (*token.TokenTransaction, error) {
	if len(request.GetTokenIds()) == 0 {
		return nil, errors.New("no token ids in RedeemRequest")
	}
	quantityToRedeem, err := ToQuantity(request.GetQuantityToRedeem())
	if err != nil {
		return nil, errors.Errorf("quantity to redeem [%d] is invalid, err '%s'", request.GetQuantityToRedeem(), err)
	}

	tokenType, quantitySum, err := t.getInputsFromTokenIds(request.GetTokenIds())
	if err != nil {
		return nil, err
	}

	cmp, err := quantitySum.Cmp(quantityToRedeem)
	if err != nil {
		return nil, errors.Errorf("cannot compare quantities '%s'", err)
	}
	if cmp < 0 {
		return nil, errors.Errorf("total quantity [%d] from TokenIds is less than quantity [%d] to be redeemed", quantitySum, request.QuantityToRedeem)
	}

	// add the output for redeem itself
	var outputs []*token.Token
	outputs = append(outputs, &token.Token{
		Type:     tokenType,
		Quantity: quantityToRedeem.ToUInt64(),
	})

	// add another output if there is remaining quantity after redemption
	if cmp > 0 {
		change, err := quantitySum.Sub(quantityToRedeem)
		if err != nil {
			return nil, errors.Errorf("failed computing change, err '%s'", err)
		}

		outputs = append(outputs, &token.Token{
			// note that tokenOwner type may change in the future depending on creator type
			Owner:    &token.TokenOwner{Type: token.TokenOwner_MSP_IDENTIFIER, Raw: t.PublicCredential}, // PublicCredential is serialized identity for the creator
			Type:     tokenType,
			Quantity: change.ToUInt64(),
		})
	}

	// Redeem shares the same data structure as Transfer
	transaction := &token.TokenTransaction{
		Action: &token.TokenTransaction_TokenAction{
			TokenAction: &token.TokenAction{
				Data: &token.TokenAction_Redeem{
					Redeem: &token.Transfer{
						Inputs:  request.GetTokenIds(),
						Outputs: outputs,
					},
				},
			},
		},
	}

	return transaction, nil
}

// read token data from ledger for each token ids and calculate the sum of quantities for all token ids
// Returns TokenIds, token type, sum of token quantities, and error in the case of failure
func (t *Transactor) getInputsFromTokenIds(tokenIds []*token.TokenId) (string, Quantity, error) {
	var tokenType = ""
	var sum = NewZeroQuantity()
	for _, tokenId := range tokenIds {
		// create the composite key from tokenId
		inKey, err := createCompositeKey(tokenOutput, []string{tokenId.TxId, strconv.Itoa(int(tokenId.Index))})
		if err != nil {
			verifierLogger.Errorf("error getting creating input key: %s", err)
			return "", nil, err
		}
		verifierLogger.Debugf("transferring token with ID: '%s'", inKey)

		// make sure the output exists in the ledger
		verifierLogger.Debugf("getting output '%s' to spend from ledger", inKey)
		inBytes, err := t.Ledger.GetState(tokenNameSpace, inKey)
		if err != nil {
			verifierLogger.Errorf("error getting output '%s' to spend from ledger: %s", inKey, err)
			return "", nil, err
		}
		if len(inBytes) == 0 {
			return "", nil, errors.New(fmt.Sprintf("input '%s' does not exist", inKey))
		}
		input := &token.Token{}
		err = proto.Unmarshal(inBytes, input)
		if err != nil {
			return "", nil, errors.New(fmt.Sprintf("error unmarshaling input bytes: '%s'", err))
		}

		// check the owner of the token
		if !bytes.Equal(t.PublicCredential, input.Owner.Raw) {
			return "", nil, errors.New(fmt.Sprintf("the requestor does not own inputs"))
		}

		// check the token type - only one type allowed per transfer
		if tokenType == "" {
			tokenType = input.Type
		} else if tokenType != input.Type {
			return "", nil, errors.New(fmt.Sprintf("two or more token types specified in input: '%s', '%s'", tokenType, input.Type))
		}

		// sum up the quantity
		quantity, err := ToQuantity(input.Quantity)
		if err != nil {
			return "", nil, errors.Errorf("quantity in input [%d] is invalid, err '%s'", input.Quantity, err)
		}

		sum, err = sum.Add(quantity)
		if err != nil {
			return "", nil, errors.Errorf("failed adding up quantities, err '%s'", err)
		}
	}

	return tokenType, sum, nil
}

// ListTokens creates a TokenTransaction that lists the unspent tokens owned by owner.
func (t *Transactor) ListTokens() (*token.UnspentTokens, error) {

	iterator, err := t.Ledger.GetStateRangeScanIterator(tokenNameSpace, "", "")
	if err != nil {
		return nil, err
	}

	tokens := make([]*token.TokenOutput, 0)
	prefix, err := createPrefix(tokenOutput)
	if err != nil {
		return nil, err
	}
	for {
		next, err := iterator.Next()

		switch {
		case err != nil:
			return nil, err

		case next == nil:
			// nil response from iterator indicates end of query results
			return &token.UnspentTokens{Tokens: tokens}, nil

		default:
			result, ok := next.(*queryresult.KV)
			if !ok {
				return nil, errors.New("failed to retrieve unspent tokens: casting error")
			}
			if strings.HasPrefix(result.Key, prefix) {
				output := &token.Token{}
				err = proto.Unmarshal(result.Value, output)
				if err != nil {
					return nil, errors.New("failed to retrieve unspent tokens: casting error")
				}
				if bytes.Equal(output.Owner.Raw, t.PublicCredential) {
					spent, err := t.isSpent(result.Key)
					if err != nil {
						return nil, err
					}
					if !spent {
						verifierLogger.Debugf("adding token with ID '%s' to list of unspent tokens", result.GetKey())
						id, err := getTokenIdFromKey(result.Key)
						if err != nil {
							return nil, err
						}
						tokens = append(tokens,
							&token.TokenOutput{
								Type:     output.Type,
								Quantity: output.Quantity,
								Id:       id,
							})
					} else {
						verifierLogger.Debugf("token with ID '%s' has been spent, not adding to list of unspent tokens", result.GetKey())
					}
				}
			}
		}
	}
}

// RequestExpectation allows indirect transfer based on the expectation.
// It creates a token transaction based on the outputs as specified in the expectation.
func (t *Transactor) RequestExpectation(request *token.ExpectationRequest) (*token.TokenTransaction, error) {
	if len(request.GetTokenIds()) == 0 {
		return nil, errors.New("no token ids in ExpectationRequest")
	}
	if request.GetExpectation() == nil {
		return nil, errors.New("no token expectation in ExpectationRequest")
	}
	if request.GetExpectation().GetPlainExpectation() == nil {
		return nil, errors.New("no plain expectation in ExpectationRequest")
	}
	if request.GetExpectation().GetPlainExpectation().GetTransferExpectation() == nil {
		return nil, errors.New("no transfer expectation in ExpectationRequest")
	}

	inputType, inputSum, err := t.getInputsFromTokenIds(request.GetTokenIds())
	if err != nil {
		return nil, err
	}

	outputs := request.GetExpectation().GetPlainExpectation().GetTransferExpectation().GetOutputs()
	outputType, outputSum, err := parseOutputs(outputs)
	if err != nil {
		return nil, err
	}
	if outputType != inputType {
		return nil, errors.Errorf("token type mismatch in inputs and outputs for expectation (%s vs %s)", outputType, inputType)
	}
	cmp, err := outputSum.Cmp(inputSum)
	if err != nil {
		return nil, errors.Errorf("cannot compare quantities '%s'", err)
	}
	if cmp > 0 {
		return nil, errors.Errorf("total quantity [%d] from TokenIds is less than total quantity [%d] in expectation", inputSum, outputSum)
	}

	// inputs may have remaining tokens after outputs - add a new output in this case
	cmp, err = inputSum.Cmp(outputSum)
	if err != nil {
		return nil, errors.Errorf("cannot compare quantities '%s'", err)
	}
	if cmp > 0 {
		change, err := inputSum.Sub(outputSum)
		if err != nil {
			return nil, errors.Errorf("failed computing change, err '%s'", err)
		}

		outputs = append(outputs, &token.Token{
			Owner:    &token.TokenOwner{Type: token.TokenOwner_MSP_IDENTIFIER, Raw: t.PublicCredential}, // PublicCredential is serialized identity for the creator
			Type:     outputType,
			Quantity: change.ToUInt64(),
		})
	}

	return &token.TokenTransaction{
		Action: &token.TokenTransaction_TokenAction{
			TokenAction: &token.TokenAction{
				Data: &token.TokenAction_Transfer{
					Transfer: &token.Transfer{
						Inputs:  request.GetTokenIds(),
						Outputs: outputs,
					},
				},
			},
		},
	}, nil
}

// Done releases any resources held by this transactor
func (t *Transactor) Done() {
	if t.Ledger != nil {
		t.Ledger.Done()
	}
}

// isSpent checks whether an output token with identifier outputID has been spent.
func (t *Transactor) isSpent(outputID string) (bool, error) {
	key, err := createInputKey(outputID)
	if err != nil {
		return false, err
	}
	result, err := t.Ledger.GetState(tokenNameSpace, key)
	if err != nil {
		return false, err
	}
	if result == nil {
		verifierLogger.Debugf("input '%s' has not been spent", key)
		return false, nil
	}
	verifierLogger.Debugf("input '%s' has already been spent", key)
	return true, nil
}

// Create a ledger key for an individual input in a token transaction, as a function of
// the outputID, which is a composite key (i.e., starts and ends with a minUnicodeRuneValue)
func createInputKey(outputID string) (string, error) {
	att := strings.Split(outputID, string(minUnicodeRuneValue))
	if len(att) < 2 {
		return "", errors.Errorf("outputID '%s' is not a valid composite key (less than two components)", outputID)
	}
	if att[0] != "" {
		return "", errors.Errorf("outputID '%s' is not a valid composite key (does not start with a component separator)", outputID)
	}
	if att[len(att)-1] != "" {
		return "", errors.Errorf("outputID '%s' is not a valid composite key (does not end with a component separator)", outputID)
	}
	if verifierLogger.IsEnabledFor(zapcore.DebugLevel) {
		for i, a := range att {
			verifierLogger.Debugf("inputID composite key attribute %d: '%s'", i, a)
		}
	}
	return createCompositeKey(tokenInput, att[2:len(att)-1])
}

// Create a prefix as a function of the string passed as argument
func createPrefix(keyword string) (string, error) {
	return createCompositeKey(keyword, nil)
}

func splitCompositeKey(compositeKey string) (string, []string, error) {
	componentIndex := 1
	components := []string{}
	for i := 1; i < len(compositeKey); i++ {
		if compositeKey[i] == minUnicodeRuneValue {
			components = append(components, compositeKey[componentIndex:i])
			componentIndex = i + 1
		}
	}
	if len(components) < 2 {
		return "", nil, errors.Errorf("invalid composite key - not enough components found in key '%s'", compositeKey)
	}
	return components[0], components[1:], nil
}

// parseOutputs iterates each output to verify token type and calculate the sum
func parseOutputs(outputs []*token.Token) (string, Quantity, error) {
	if len(outputs) == 0 {
		return "", nil, errors.New("no outputs in request")
	}

	outputType := ""
	outputSum := NewZeroQuantity()
	for _, output := range outputs {
		if outputType == "" {
			outputType = output.GetType()
		} else if outputType != output.GetType() {
			return "", nil, errors.Errorf("multiple token types ('%s', '%s') in outputs", outputType, output.GetType())
		}
		quantity, err := ToQuantity(output.GetQuantity())
		if err != nil {
			return "", nil, errors.Errorf("quantity in output [%d] is invalid, err '%s'", output.GetQuantity(), err)
		}

		outputSum, err = outputSum.Add(quantity)
		if err != nil {
			return "", nil, errors.Errorf("failed adding up quantities, err '%s'", err)
		}
	}

	return outputType, outputSum, nil
}

func getTokenIdFromKey(key string) (*token.TokenId, error) {
	_, components, err := splitCompositeKey(key)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("error splitting input composite key: '%s'", err))
	}

	if len(components) != 2 {
		return nil, errors.New(fmt.Sprintf("not enough components in output ID composite key; expected 2, received '%s'", components))
	}

	txID := components[0]
	index, err := strconv.Atoi(components[1])
	if err != nil {
		return nil, errors.New(fmt.Sprintf("error parsing output index '%s': '%s'", components[1], err))
	}
	return &token.TokenId{TxId: txID, Index: uint32(index)}, nil
}
