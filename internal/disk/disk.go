package disk

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/tita-n/atomdb/internal/atom"
)

const timestampLayout = time.RFC3339Nano

type diskAtom struct {
	Entity    string      `json:"entity"`
	Attribute string      `json:"attribute"`
	Value     interface{} `json:"value"`
	Type      string      `json:"type"`
	Timestamp string      `json:"timestamp"`
	Version   int64       `json:"version"`
}

func Save(a *atom.Atom, file *os.File) error {
	da := diskAtom{
		Entity:    a.Entity,
		Attribute: a.Attribute,
		Value:     a.Value,
		Type:      a.Type,
		Timestamp: a.Timestamp.Format(timestampLayout),
		Version:   a.Version,
	}

	data, err := json.Marshal(da)
	if err != nil {
		return fmt.Errorf("failed to marshal atom: %w", err)
	}

	data = append(data, '\n')

	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("failed to write atom to file: %w", err)
	}

	return nil
}

func Sync(file *os.File) error {
	return file.Sync()
}

func truncateError(err error) string {
	msg := err.Error()
	if len(msg) > 120 {
		msg = msg[:120] + "..."
	}
	return msg
}

func Load(path string) ([]atom.Atom, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	const maxLineSize = 1024 * 1024
	bufSize := maxLineSize + 4096

	var atoms []atom.Atom
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, bufSize), bufSize)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}

		var da diskAtom
		if err := json.Unmarshal(line, &da); err != nil {
			log.Printf("WARNING: skipping corrupt line %d: %v", lineNum, truncateError(err))
			continue
		}

		// Validate required fields
		if da.Entity == "" || da.Attribute == "" {
			log.Printf("WARNING: skipping line %d: missing required fields (entity or attribute empty)", lineNum)
			continue
		}

		// Validate type is a known value
		switch da.Type {
		case "string", "number", "boolean", "ref", "timestamp", "deleted", "":
			// valid
		default:
			log.Printf("WARNING: skipping line %d: unknown atom type", lineNum)
			continue
		}

		// Validate value size for strings
		if s, ok := da.Value.(string); ok {
			if len(s) > 1048576 { // MaxValueLength
				log.Printf("WARNING: skipping line %d: value exceeds maximum length", lineNum)
				continue
			}
		}

		a := atom.Atom{
			Entity:    da.Entity,
			Attribute: da.Attribute,
			Value:     da.Value,
			Type:      da.Type,
			Version:   da.Version,
		}

		ts, err := time.Parse(timestampLayout, da.Timestamp)
		if err != nil {
			ts, err = time.Parse("2006-01-02 15:04:05", da.Timestamp)
			if err != nil {
				ts = time.Time{}
			}
		}
		a.Timestamp = ts

		atoms = append(atoms, a)
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error reading file: %w", err)
	}

	return atoms, nil
}

func Compact(path string, atoms map[string]map[string]*atom.Atom, keepTombstones bool) error {
	tmpFile, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".compact.*")
	if err != nil {
		return fmt.Errorf("failed to create temp file: %w", err)
	}
	tmpPath := tmpFile.Name()

	defer func() {
		if err != nil {
			tmpFile.Close()
			os.Remove(tmpPath)
		}
	}()

	// Explicitly set permissions on temp file
	if err = tmpFile.Chmod(0600); err != nil {
		return fmt.Errorf("failed to set temp file permissions: %w", err)
	}

	for _, attrs := range atoms {
		for _, a := range attrs {
			if a.Type == "deleted" && !keepTombstones {
				continue
			}
			if err = Save(a, tmpFile); err != nil {
				return fmt.Errorf("failed to write during compaction: %w", err)
			}
		}
	}

	if err = tmpFile.Sync(); err != nil {
		return fmt.Errorf("failed to sync temp file: %w", err)
	}
	if err = tmpFile.Close(); err != nil {
		return fmt.Errorf("failed to close temp file: %w", err)
	}

	// On Windows, remove existing file first (os.Rename doesn't overwrite)
	os.Remove(path)
	if err = os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("failed to replace data file: %w", err)
	}

	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		dir.Sync()
		dir.Close()
	}

	return nil
}

func CleanupOrphaned(path string) {
	dir := filepath.Dir(path)
	prefix := filepath.Base(path) + ".compact."
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() && len(e.Name()) > len(prefix) && e.Name()[:len(prefix)] == prefix {
			full := filepath.Join(dir, e.Name())
			log.Printf("WARNING: removing orphaned compaction file")
			os.Remove(full)
		}
	}
}
