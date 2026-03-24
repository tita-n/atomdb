package store

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/tita-n/atomdb/internal/atom"
)

// Backup represents a point-in-time snapshot of the store.
type Backup struct {
	Timestamp time.Time               `json:"timestamp"`
	Entities  map[string]BackupEntity `json:"entities"`
}

// BackupEntity is a serializable entity.
type BackupEntity struct {
	Attributes map[string]BackupAtom `json:"attributes"`
}

// BackupAtom is a serializable atom.
type BackupAtom struct {
	Value     interface{} `json:"value"`
	Type      string      `json:"type"`
	Timestamp string      `json:"timestamp"`
	Version   int64       `json:"version"`
}

// Backup creates a point-in-time backup of the store.
func (s *AtomStore) Backup(path string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	backup := Backup{
		Timestamp: time.Now(),
		Entities:  make(map[string]BackupEntity, len(s.atoms)),
	}

	for entity, attrs := range s.atoms {
		be := BackupEntity{
			Attributes: make(map[string]BackupAtom, len(attrs)),
		}
		for attr, a := range attrs {
			if a.Type == "deleted" {
				continue
			}
			be.Attributes[attr] = BackupAtom{
				Value:     a.Value,
				Type:      a.Type,
				Timestamp: a.Timestamp.Format(time.RFC3339Nano),
				Version:   a.Version,
			}
		}
		backup.Entities[entity] = be
	}

	data, err := json.MarshalIndent(backup, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal backup: %w", err)
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0700); err != nil {
		return fmt.Errorf("failed to create backup directory: %w", err)
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0600); err != nil {
		return fmt.Errorf("failed to write backup file: %w", err)
	}

	if err := os.Rename(tmpPath, path); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("failed to finalize backup: %w", err)
	}

	return nil
}

// Restore loads a backup into the store, replacing all existing data.
func (s *AtomStore) Restore(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return fmt.Errorf("failed to read backup: %w", err)
	}

	var backup Backup
	if err := json.Unmarshal(data, &backup); err != nil {
		return fmt.Errorf("failed to unmarshal backup: %w", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// Clear existing data
	s.atoms = make(map[string]map[string]*atom.Atom)
	s.idx.RebuildFromAtoms(s.atoms)
	s.history = s.history[:0]

	// Restore from backup
	for entity, be := range backup.Entities {
		s.atoms[entity] = make(map[string]*atom.Atom)
		for attr, ba := range be.Attributes {
			ts, err := time.Parse(time.RFC3339Nano, ba.Timestamp)
			if err != nil {
				ts = time.Now()
			}
			a := &atom.Atom{
				Entity:    entity,
				Attribute: attr,
				Value:     ba.Value,
				Type:      ba.Type,
				Timestamp: ts,
				Version:   ba.Version,
			}
			s.atoms[entity][attr] = a
			s.idx.IndexAtom(a)
		}
	}

	s.dirty = true
	return s.maybeSync()
}

// BackupList returns all backup files in a directory.
func BackupList(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var backups []string
	for _, e := range entries {
		if !e.IsDir() && filepath.Ext(e.Name()) == ".bak" {
			backups = append(backups, filepath.Join(dir, e.Name()))
		}
	}
	return backups, nil
}
