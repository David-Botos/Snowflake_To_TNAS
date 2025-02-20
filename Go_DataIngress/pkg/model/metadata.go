// pkg/model/metadata.go
package model

import "strings"

// TableMetadata contains the structure information for a database table
type TableMetadata struct {
	Schema      string   // Schema name
	Table       string   // Table name
	Columns     []Column // Column definitions
	PrimaryKeys []string // List of primary key column names
}

// Column represents metadata about a database column
type Column struct {
	Name         string // Column name
	DataType     string // Original data type (from Snowflake)
	PgType       string // Mapped PostgreSQL type
	Nullable     bool   // Whether column allows NULL values
	IsPrimaryKey bool   // Whether column is part of primary key
}

// GetColumnByName returns a column by name (case-insensitive)
// Returns nil if column not found
func (tm *TableMetadata) GetColumnByName(name string) *Column {
	normalizedName := normalizeColumnName(name)
	for i, col := range tm.Columns {
		if normalizeColumnName(col.Name) == normalizedName {
			return &tm.Columns[i]
		}
	}
	return nil
}

// IsUUIDColumn checks if a column should be treated as a UUID
// based on name pattern or data type
func (col *Column) IsUUIDColumn() bool {
	name := normalizeColumnName(col.Name)

	// Check name patterns
	if name == "uuid" || name == "id" ||
		hasSuffix(name, "_uuid") || hasSuffix(name, "_id") {
		return true
	}

	// Check data type
	dataType := normalizeColumnName(col.DataType)
	return contains(dataType, "uuid")
}

// Helper functions for case-insensitive string operations
func normalizeColumnName(name string) string {
	// Converting to lowercase is a simple normalization
	// You could add more complex normalization if needed
	return strings.ToLower(name)
}

func contains(s, substr string) bool {
	return strings.Contains(
		strings.ToLower(s),
		strings.ToLower(substr),
	)
}

func hasSuffix(s, suffix string) bool {
	return strings.HasSuffix(
		strings.ToLower(s),
		strings.ToLower(suffix),
	)
}

// TODO: Future enhancements might include:
// - Schema validation methods
// - Default value handling
// - Foreign key relationships
// - Index information
