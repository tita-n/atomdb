# AtomDB

A local-first database built on atoms. Every piece of data is an atom with Entity, Attribute, Value, Type, Timestamp, and Version.

## Features

- **EAV Model** — Flexible schema: entities can have any attributes
- **B-Tree Indexes** — Per-attribute secondary indexes for fast queries
- **Range Queries** — Numeric and string range queries (`>`, `<`, `>=`, `<=`)
- **Compound Queries** — `AND`/`OR` combining conditions
- **Full-Text Search** — Inverted index with stop words
- **Append-Only Storage** — Crash-safe JSON lines file
- **Compaction** — Rewrite file to reclaim space from deletes

## Quick Start

```bash
go build -o atomdb ./cmd/atomdb/

# Store atoms
./atomdb set user:1 name "Ayo" string
./atomdb set user:1 age 28 number
./atomdb set user:2 age 35 number

# Retrieve
./atomdb get user:1 name
./atomdb getall user:1

# Query with indexes
./atomdb query age ">" 25
./atomdb query age ">=" 25 AND city "==" Lagos

# Explain query plan
./atomdb explain age ">" 30

# Full-text search
./atomdb set user:1 name "Ayo Adeleke" string
./atomdb search name contains Ayo

# Management
./atomdb stats
./atomdb index list
./atomdb compact
```

## Commands

| Command | Description |
|---------|-------------|
| `set <entity> <attribute> <value> <type>` | Store an atom |
| `get <entity> <attribute>` | Retrieve an atom |
| `getall <entity>` | Get all attributes for entity |
| `query <attr> <op> <value> [AND\|OR ...]` | Query atoms |
| `explain <attr> <op> <value>` | Show query plan |
| `delete <entity> <attribute>` | Delete an atom |
| `search <attr> contains <word>` | Full-text search |
| `index list\|rebuild` | Manage indexes |
| `stats` | Store statistics |
| `compact` | Compact data file |

## Types

`string`, `number`, `boolean`, `ref`, `timestamp`

## Shell Usage

Operators `>`, `<` are shell metacharacters. Quote them:

```bash
./atomdb query age ">" 25      # bash
./atomdb query age '>' 25      # also works
.\atomdb.exe query age ">" 25  # PowerShell
```

## Data File

Data is stored as append-only JSON lines in `data.db` (configurable via `ATOMDB_PATH` env var). Each line is one atom. Compaction rewrites the file keeping only live data.

## Architecture

```
atomdb/
├── cmd/atomdb/main.go          # Entry point, path sanitization
├── cli/cli.go                  # CLI command dispatcher
├── internal/
│   ├── atom/atom.go            # Atom struct, validation
│   ├── disk/disk.go            # Append-only JSON lines I/O, compaction
│   ├── index/
│   │   ├── btree.go            # B-Tree (order 32) with range queries
│   │   └── index.go            # Index manager, inverted text index
│   └── store/store.go          # Store with B-Tree integration
```

## License

MIT
