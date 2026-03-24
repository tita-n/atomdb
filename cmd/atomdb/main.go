package main

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"unicode/utf8"

	"github.com/tita-n/atomdb/cli"
	"github.com/tita-n/atomdb/internal/schema"
	"github.com/tita-n/atomdb/internal/store"
)

const defaultDBPath = "data.db"
const defaultDataDir = "data"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %s\n", sanitizeError(err))
		os.Exit(1)
	}
}

func run() error {
	dbPath := os.Getenv("ATOMDB_PATH")
	if dbPath == "" {
		dbPath = defaultDBPath
	}

	sanitized, err := sanitizePath(dbPath)
	if err != nil {
		return err
	}
	dbPath = sanitized

	// Resolve data directory path first, then validate it's within allowed bounds
	dataDir := filepath.Dir(dbPath)
	if dataDir == "." {
		dataDir = defaultDataDir
	}

	// Always ensure data directory exists with restricted permissions
	if err := os.MkdirAll(dataDir, 0700); err != nil {
		return fmt.Errorf("failed to create data directory: %w", err)
	}

	// Validate the data directory path is not a symlink/traversal on Windows
	resolvedDir, err := filepath.Abs(dataDir)
	if err != nil {
		return fmt.Errorf("failed to resolve data directory: %w", err)
	}
	if err := validateDirPath(resolvedDir); err != nil {
		return fmt.Errorf("invalid data directory: %w", err)
	}

	s, err := store.NewWithMode(dbPath, store.SyncBatch)
	if err != nil {
		return fmt.Errorf("failed to open store: %w", err)
	}
	defer s.Close()

	sc := schema.New()
	sc.LoadFromFile(dataDir + "/schema.json")

	db := cli.NewDB(s, sc, dataDir)

	return cli.Run(db, os.Args[1:])
}

func sanitizePath(p string) (string, error) {
	if !utf8.ValidString(p) {
		return "", fmt.Errorf("invalid database path: not valid UTF-8")
	}
	if strings.ContainsRune(p, 0) {
		return "", fmt.Errorf("invalid database path: contains null byte")
	}
	p = filepath.Clean(p)

	// On Windows, filepath.IsAbs returns false for drive-relative paths like "C:foo"
	// We need to resolve and validate containment after cleaning
	if filepath.IsAbs(p) {
		return "", fmt.Errorf("invalid database path: must not be absolute")
	}

	// Check for ".." after cleaning (covers C:..\foo patterns)
	if strings.Contains(p, "..") {
		return "", fmt.Errorf("invalid database path: must not contain '..'")
	}

	// Validate Windows drive-relative paths don't escape intended directory
	if err := validateDirPath(p); err != nil {
		return "", fmt.Errorf("invalid database path: %w", err)
	}

	return p, nil
}

func validateDirPath(p string) error {
	// On Windows, resolve to absolute and verify it's within expected bounds
	absPath, err := filepath.Abs(p)
	if err != nil {
		return fmt.Errorf("cannot resolve path: %w", err)
	}

	// Check for Windows drive letter root escape (e.g., C:foo -> C:\currentdir\foo -> can reach C:\)
	// The cleaned path should not start with a bare drive like "C:" without a separator
	cleaned := filepath.Clean(p)
	if len(cleaned) >= 2 && cleaned[1] == ':' && (len(cleaned) == 2 || cleaned[2] != '\\' && cleaned[2] != '/') {
		return fmt.Errorf("path escapes root directory")
	}

	// Verify the absolute path doesn't have unexpected drive roots
	if runtime.GOOS == "windows" {
		vol := filepath.VolumeName(absPath)
		if vol != "" && !strings.HasPrefix(cleaned, vol) && !strings.HasPrefix(cleaned, "."+string(filepath.Separator)) {
			// Path like "C:foo" where working dir is on different drive is suspicious
			if strings.HasPrefix(cleaned, ".:") || (len(cleaned) >= 3 && cleaned[0] != '.' && cleaned[1] == ':' && cleaned[2] != '\\' && cleaned[2] != '/') {
				return fmt.Errorf("drive-relative path not allowed")
			}
		}
	}

	return nil
}

func sanitizeError(err error) string {
	msg := err.Error()
	var sb strings.Builder
	sb.Grow(len(msg))
	for _, r := range msg {
		if r < 32 && r != '\t' {
			continue
		}
		if r == 127 {
			continue
		}
		sb.WriteRune(r)
	}
	msg = sb.String()
	msg = strings.ReplaceAll(msg, "\n", " ")
	msg = strings.ReplaceAll(msg, "\r", " ")
	if len(msg) > 200 {
		msg = msg[:200] + "..."
	}
	return msg
}
