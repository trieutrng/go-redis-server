package main

type TxStatus string

const (
	TxActive    TxStatus = "ACTIVE"
	TxExecuting TxStatus = "EXECUTING"
)

type Transaction struct {
	Active map[string]*TxUnit
}

type TxUnit struct {
	Status TxStatus
	Queued [][]byte
}

func NewTransaction() *Transaction {
	return &Transaction{
		Active: make(map[string]*TxUnit),
	}
}

func (tx *Transaction) Start(txId string) {
	tx.Active[txId] = &TxUnit{
		Status: TxActive,
		Queued: make([][]byte, 0),
	}
}

func (tx *Transaction) Enqueue(txId string, cmd []byte) {
	txUnit, ok := tx.Active[txId]
	if !ok {
		return
	}
	txUnit.Queued = append(txUnit.Queued, cmd)
	tx.Active[txId] = txUnit
}

func (tx *Transaction) IsExisted(txId string) bool {
	_, ok := tx.Active[txId]
	return ok
}

func (tx *Transaction) Inactive(txId string) {
	_, ok := tx.Active[txId]
	if ok {
		delete(tx.Active, txId)
	}
}

func (tx *Transaction) GetTx(txId string) *TxUnit {
	queued, ok := tx.Active[txId]
	if !ok {
		return nil
	}
	return queued
}

func (tx *Transaction) ChangeTxStatus(txId string, txStatus TxStatus) {
	txUnit, ok := tx.Active[txId]
	if !ok {
		return
	}
	txUnit.Status = txStatus
	tx.Active[txId] = txUnit
}
