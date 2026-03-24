package store

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/tita-n/atomdb/internal/atom"
)

// TransactionID is a unique identifier for a transaction.
type TransactionID uint64

var txCounter atomic.Uint64

// NextTransactionID returns the next unique transaction ID.
func NextTransactionID() TransactionID {
	return TransactionID(txCounter.Add(1))
}

// Operation represents a single write within a transaction.
type Operation struct {
	Type      string // "set" or "delete"
	Entity    string
	Attribute string
	Value     interface{}
	ValueType string
	OldAtom   *atom.Atom // previous value for rollback
}

// Transaction represents an atomic unit of work.
type Transaction struct {
	ID         TransactionID
	Store      *AtomStore
	Operations []Operation
	Committed  bool
	RolledBack bool
	mu         sync.Mutex
}

// BeginTx starts a new transaction.
func (s *AtomStore) BeginTx() *Transaction {
	return &Transaction{
		ID:    NextTransactionID(),
		Store: s,
	}
}

// Set queues a set operation within the transaction.
func (tx *Transaction) Set(entity, attribute string, value interface{}, valueType string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Committed || tx.RolledBack {
		return fmt.Errorf("transaction %d already finalized", tx.ID)
	}

	// Capture old value for rollback
	var oldAtom *atom.Atom
	if existing, ok := tx.Store.Get(entity, attribute); ok {
		c := *existing
		oldAtom = &c
	}

	tx.Operations = append(tx.Operations, Operation{
		Type:      "set",
		Entity:    entity,
		Attribute: attribute,
		Value:     value,
		ValueType: valueType,
		OldAtom:   oldAtom,
	})
	return nil
}

// Delete queues a delete operation within the transaction.
func (tx *Transaction) Delete(entity, attribute string) error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Committed || tx.RolledBack {
		return fmt.Errorf("transaction %d already finalized", tx.ID)
	}

	var oldAtom *atom.Atom
	if existing, ok := tx.Store.Get(entity, attribute); ok {
		c := *existing
		oldAtom = &c
	} else {
		return ErrNotFound
	}

	tx.Operations = append(tx.Operations, Operation{
		Type:      "delete",
		Entity:    entity,
		Attribute: attribute,
		OldAtom:   oldAtom,
	})
	return nil
}

// Commit applies all operations atomically.
func (tx *Transaction) Commit() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Committed || tx.RolledBack {
		return fmt.Errorf("transaction %d already finalized", tx.ID)
	}

	// Acquire store lock for the entire commit
	tx.Store.mu.Lock()
	defer tx.Store.mu.Unlock()

	// Apply all operations
	for _, op := range tx.Operations {
		switch op.Type {
		case "set":
			a, err := atom.NewAtom(op.Entity, op.Attribute, op.Value, op.ValueType)
			if err != nil {
				// Rollback on error
				tx.rollbackLocked()
				return fmt.Errorf("transaction %d commit failed: %w", tx.ID, err)
			}
			// Remove old from index
			if oldAttrs, ok := tx.Store.atoms[op.Entity]; ok {
				if old, exists := oldAttrs[op.Attribute]; exists && old.Type != "deleted" {
					tx.Store.idx.RemoveAtom(old)
				}
			}
			tx.Store.applyAtom(a)
			tx.Store.idx.IndexAtom(a)
			tx.Store.appendHistory(*a)
			tx.Store.dirty = true
			tx.Store.dirtyCount++

		case "delete":
			tombstone, err := atom.NewAtom(op.Entity, op.Attribute, nil, "deleted")
			if err != nil {
				tx.rollbackLocked()
				return fmt.Errorf("transaction %d commit failed: %w", tx.ID, err)
			}
			if oldAttrs, ok := tx.Store.atoms[op.Entity]; ok {
				if old, exists := oldAttrs[op.Attribute]; exists {
					tx.Store.idx.RemoveAtom(old)
				}
			}
			tx.Store.applyAtom(tombstone)
			tx.Store.appendHistory(*tombstone)
			tx.Store.dirty = true
			tx.Store.dirtyCount++
		}
	}

	tx.Committed = true
	return tx.Store.maybeSync()
}

// Rollback discards all operations.
func (tx *Transaction) Rollback() error {
	tx.mu.Lock()
	defer tx.mu.Unlock()

	if tx.Committed || tx.RolledBack {
		return fmt.Errorf("transaction %d already finalized", tx.ID)
	}

	tx.RolledBack = true
	tx.Operations = nil
	return nil
}

// rollbackLocked performs rollback while already holding the store lock.
func (tx *Transaction) rollbackLocked() {
	tx.RolledBack = true
	tx.Operations = nil
}

// WithTransaction executes a function within a transaction.
// Commits on success, rolls back on error.
func (s *AtomStore) WithTransaction(fn func(tx *Transaction) error) error {
	tx := s.BeginTx()
	if err := fn(tx); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}
