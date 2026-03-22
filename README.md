# AtomDB

A modern database with clean syntax. Type-safe, indexed, and local-first.

## Quick Start

```bash
go build -o atomdb ./cmd/atomdb/

# Define a type
atomdb type person {name:string,age:number,city:string}

# Insert records
atomdb insert person name:John age:28 city:Lagos
atomdb insert person name:Ayo age:35 city:Abuja

# Query
atomdb person                          # all records
atomdb person where age > 25           # filtered
atomdb person.name where city == Lagos # specific fields

# Update
atomdb update person where name == John set age=30

# Delete
atomdb delete person where name == Ayo
```

## Syntax

| Operation | Syntax |
|-----------|--------|
| Define type | `type name { field: type, field: type? }` |
| Insert | `insert type field:value field:value` |
| Query | `type [fields] where condition` |
| Update | `update type where condition set field=value` |
| Delete | `delete type where condition` |
| List types | `types` |

### Field Types
- `string` — text
- `number` — numeric
- `boolean` — true/false
- `ref(type)` — reference to another type
- `val1|val2|val3` — enum (limited options)
- `field?` — optional field
- `field = default` — default value

### Operators
`==` `!=` `>` `<` `>=` `<=`

## Raw Commands (Backward Compatible)

```bash
atomdb set entity attribute value type
atomdb get entity attribute
atomdb getall entity
atomdb query attribute op value
atomdb stats
atomdb compact
```

## Architecture

```
atomdb/
├── cmd/atomdb/main.go           # Entry point
├── cli/cli.go                   # CLI with type-aware commands
├── internal/
│   ├── atom/atom.go             # Atom struct (EAV storage)
│   ├── disk/disk.go             # Append-only JSON lines I/O
│   ├── index/
│   │   ├── btree.go             # B-Tree (order 32) with range queries
│   │   └── index.go             # Index manager, text search
│   ├── query/query.go           # Query parser and executor
│   ├── schema/schema.go         # Type definitions and validation
│   └── store/store.go           # Storage engine
```

## License

MIT
