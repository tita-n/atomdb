package disk

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/user/atomdb/internal/atom"
)

// Timestamp format with nanosecond precision.
// Using RFC3339Nano preserves full time.Time precision
// which is critical for correct version ordering.
const timestampLayout = time.RFC3339Nano

// diskAtom is an internal representation used for JSON serialization.
// We use a separate type because json.Marshal doesn't handle
// interface{} values with type information well - we need to
// ensure Value round-trips correctly through JSON.
type diskAtom struct {
	Entity    string      `json:"entity"`
	Attribute string      `json:"attribute"`
	Value     interface{} `json:"value"`
	Type      string      `json:"type"`
	Timestamp string      `json:"timestamp"`
	Version   int64       `json:"version"`
	NodeID    string      `json:"node_id,omitempty"`
}

// Save appends a single atom as a JSON line to the file.
// Does NOT call file.Sync() - callers should batch writes and
// call Sync() themselves for performance.
// Design decision: append-only format means we never modify
// existing data in place. This guarantees crash safety -
// if the process dies mid-write, at worst we get a partial
// last line which Load handles by skipping invalid entries.
func Save(a *atom.Atom, file *os.File) error {
	da := diskAtom{
		Entity:    a.Entity,
		Attribute: a.Attribute,
		Value:     a.Value,
		Type:      a.Type,
		Timestamp: a.Timestamp.Format(timestampLayout),
		Version:   a.Version,
		NodeID:    a.NodeID,
	}

	data, err := json.Marshal(da)
	if err != nil {
		return fmt.Errorf("failed to marshal atom: %w", err)
	}

	// Append newline to delimit records
	data = append(data, '\n')

	// Write atomically to the file. Since we're appending a single
	// line with a newline, partial writes result in truncated JSON
	// that Load will skip - no corruption of existing data.
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

// Load reads all atoms from the file.
// Design decision: we scan line by line rather than reading the
// whole file into memory because the append-only log can grow large.
// Invalid lines (from partial writes during crashes) are logged
// and skipped - this is the crash recovery mechanism.
func Load(path string) ([]atom.Atom, error) {
	file, err := os.Open(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // First run, no file yet
		}
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Buffer sized to handle individual lines up to 1MB.
	// bufio.Scanner uses this for a single line, not the whole file.
	// 1MB accommodates atoms with very large values (e.g. base64 blobs).
	const maxLineSize = 1024 * 1024 // 1MB per line
	bufSize := maxLineSize

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

		a := atom.Atom{
			Entity:    da.Entity,
			Attribute: da.Attribute,
			Value:     da.Value,
			Type:      da.Type,
			Version:   da.Version,
			NodeID:    da.NodeID,
		}

		// Parse timestamp with nanosecond precision
		ts, err := time.Parse(timestampLayout, da.Timestamp)
		if err != nil {
			// Fallback for old format (seconds only)
			ts, err = time.Parse("2006-01-02 15:04:05", da.Timestamp)
			if err != nil {
				log.Printf("WARNING: unparseable timestamp on line %d", lineNum)
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

	for _, attrs := range atoms {
		for _, a := range attrs {
			if a.Type == "deleted" && !keepTombstones {
				continue
			}
			if err = Save(a, tmpFile); err != nil {
				err = fmt.Errorf("failed to write during compaction: %w", err)
				return err
			}
		}
	}

	if err = tmpFile.Sync(); err != nil {
		err = fmt.Errorf("failed to sync temp file: %w", err)
		return err
	}
	if err = tmpFile.Close(); err != nil {
		err = fmt.Errorf("failed to close temp file: %w", err)
		return err
	}

	if err = os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("failed to replace data file: %w", err)
	}

	if dir, err := os.Open(filepath.Dir(path)); err == nil {
		dir.Sync()
		dir.Close()
	}

	return nil
}

// CleanupOrphaned removes leftover compact temp files from crashed compactions.
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
