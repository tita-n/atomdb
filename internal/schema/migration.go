package schema

import (
	"encoding/json"
	"fmt"
	"os"
	"time"
)

// Migration represents a schema change.
type Migration struct {
	Version   int       `json:"version"`
	Name      string    `json:"name"`
	Timestamp time.Time `json:"timestamp"`
	UpSQL     string    `json:"up_sql"`   // description of forward migration
	DownSQL   string    `json:"down_sql"` // description of rollback
	Changes   []Change  `json:"changes"`
}

// Change represents a single field-level change in a migration.
type Change struct {
	Type       string      `json:"type"` // add_field, remove_field, rename_field, change_type
	FieldName  string      `json:"field_name"`
	NewName    string      `json:"new_name,omitempty"`
	NewType    string      `json:"new_type,omitempty"`
	OldType    string      `json:"old_type,omitempty"`
	Optional   bool        `json:"optional,omitempty"`
	DefaultVal interface{} `json:"default_val,omitempty"`
}

// MigrationLog tracks applied migrations.
type MigrationLog struct {
	migrations []Migration
	path       string
}

// NewMigrationLog creates a migration log backed by the given file.
func NewMigrationLog(path string) *MigrationLog {
	return &MigrationLog{
		path: path,
	}
}

// Load reads the migration log from disk.
func (ml *MigrationLog) Load() error {
	data, err := os.ReadFile(ml.path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	return json.Unmarshal(data, &ml.migrations)
}

// Save writes the migration log to disk.
func (ml *MigrationLog) Save() error {
	data, err := json.MarshalIndent(ml.migrations, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(ml.path, data, 0600)
}

// CurrentVersion returns the latest applied migration version.
func (ml *MigrationLog) CurrentVersion() int {
	if len(ml.migrations) == 0 {
		return 0
	}
	return ml.migrations[len(ml.migrations)-1].Version
}

// Applied returns all applied migrations.
func (ml *MigrationLog) Applied() []Migration {
	return ml.migrations
}

// Record adds a migration to the log.
func (ml *MigrationLog) Record(m Migration) {
	ml.migrations = append(ml.migrations, m)
}

// AddField creates a migration that adds a new field to a type.
func (ml *MigrationLog) AddField(typeName string, field FieldDef) Migration {
	m := Migration{
		Version:   ml.CurrentVersion() + 1,
		Name:      fmt.Sprintf("add_%s_to_%s", field.Name, typeName),
		Timestamp: time.Now(),
		Changes: []Change{{
			Type:       "add_field",
			FieldName:  field.Name,
			NewType:    field.Type.String(),
			Optional:   field.Optional,
			DefaultVal: field.Default,
		}},
	}
	return m
}

// RemoveField creates a migration that removes a field from a type.
func (ml *MigrationLog) RemoveField(typeName string, fieldName string) Migration {
	m := Migration{
		Version:   ml.CurrentVersion() + 1,
		Name:      fmt.Sprintf("remove_%s_from_%s", fieldName, typeName),
		Timestamp: time.Now(),
		Changes: []Change{{
			Type:      "remove_field",
			FieldName: fieldName,
		}},
	}
	return m
}

// RenameField creates a migration that renames a field.
func (ml *MigrationLog) RenameField(typeName, oldName, newName string) Migration {
	m := Migration{
		Version:   ml.CurrentVersion() + 1,
		Name:      fmt.Sprintf("rename_%s_to_%s_in_%s", oldName, newName, typeName),
		Timestamp: time.Now(),
		Changes: []Change{{
			Type:      "rename_field",
			FieldName: oldName,
			NewName:   newName,
		}},
	}
	return m
}

// ApplyMigration applies a migration to the schema.
func (s *Schema) ApplyMigration(typeName string, m Migration) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	td, ok := s.types[typeName]
	if !ok {
		return fmt.Errorf("type %q not found", typeName)
	}

	for _, change := range m.Changes {
		switch change.Type {
		case "add_field":
			// Check if field already exists
			for _, f := range td.Fields {
				if f.Name == change.FieldName {
					return fmt.Errorf("field %q already exists in type %q", change.FieldName, typeName)
				}
			}
			newField := FieldDef{
				Name:     change.FieldName,
				Type:     ParseFieldType(change.NewType),
				Optional: change.Optional,
				Default:  change.DefaultVal,
			}
			td.Fields = append(td.Fields, newField)

		case "remove_field":
			found := false
			for i, f := range td.Fields {
				if f.Name == change.FieldName {
					td.Fields = append(td.Fields[:i], td.Fields[i+1:]...)
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("field %q not found in type %q", change.FieldName, typeName)
			}

		case "rename_field":
			found := false
			for i, f := range td.Fields {
				if f.Name == change.FieldName {
					td.Fields[i].Name = change.NewName
					found = true
					break
				}
			}
			if !found {
				return fmt.Errorf("field %q not found in type %q", change.FieldName, typeName)
			}

		default:
			return fmt.Errorf("unknown change type: %q", change.Type)
		}
	}

	return nil
}
