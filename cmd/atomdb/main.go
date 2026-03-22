package main

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"unicode/utf8"

	"github.com/tita-n/atomdb/cli"
	"github.com/tita-n/atomdb/internal/store"
)

const defaultDBPath = "data.db"

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

	s, err := store.NewWithMode(dbPath, store.SyncBatch)
	if err != nil {
		return fmt.Errorf("failed to open store: %w", err)
	}
	defer s.Close()

	return cli.Run(s, os.Args[1:])
}

func sanitizePath(p string) (string, error) {
	if !utf8.ValidString(p) {
		return "", fmt.Errorf("invalid database path: not valid UTF-8")
	}
	if strings.ContainsRune(p, 0) {
		return "", fmt.Errorf("invalid database path: contains null byte")
	}

	p = filepath.Clean(p)

	if filepath.IsAbs(p) {
		return "", fmt.Errorf("invalid database path: must not be absolute")
	}
	if strings.Contains(p, "..") {
		return "", fmt.Errorf("invalid database path: must not contain '..'")
	}

	return p, nil
}

func sanitizeError(err error) string {
	msg := err.Error()
	msg = strings.ReplaceAll(msg, "\n", " ")
	msg = strings.ReplaceAll(msg, "\r", " ")
	if len(msg) > 200 {
		msg = msg[:200] + "..."
	}
	return msg
}
