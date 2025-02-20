// pkg/model/cleaning.go
package model

import (
	"time"
)

// CleaningOperation represents a single data cleaning operation
type CleaningOperation struct {
	SchemaName        string      // Database schema name
	TableName         string      // Table name
	ColumnName        string      // Column that was cleaned
	OriginalValue     interface{} // Original value (may be nil)
	NewValue          string      // New value after cleaning
	RowIdentifier     string      // ID that identifies the row (usually ID column)
	CleaningOperation string      // Type of cleaning performed (e.g., "uuid_generation")
	CleaningReason    string      // Reason for cleaning (e.g., "missing_primary_key")
	CleanedAt         time.Time   // When the cleaning occurred (set by database)
}

// CleaningContext contains information needed for cleaning a value
type CleaningContext struct {
	SchemaName    string
	TableName     string
	ColumnName    string
	RowIdentifier string
	IsIDColumn    bool
	DataType      string
}
