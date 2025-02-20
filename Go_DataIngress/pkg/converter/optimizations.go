// pkg/converter/optimizations.go
package converter

import (
	"fmt"
	"strings"

	"github.com/David-Botos/data-ingress/pkg/model"

	"go.uber.org/zap"
)

// OptimizeTableMetadata analyzes and optimizes table metadata
func (c *TypeConverter) OptimizeTableMetadata(metadata *model.TableMetadata) *model.TableMetadata {
	if !c.config.OptimizeStorage {
		return metadata
	}

	optimized := &model.TableMetadata{
		Schema:      metadata.Schema,
		Table:       metadata.Table,
		Columns:     make([]model.Column, len(metadata.Columns)),
		PrimaryKeys: metadata.PrimaryKeys,
	}

	for i, col := range metadata.Columns {
		optimized.Columns[i] = c.optimizeColumn(col)
	}

	return optimized
}

// optimizeColumn applies storage optimizations to a column
func (c *TypeConverter) optimizeColumn(col model.Column) model.Column {
	optimized := col

	// Convert oversized VARCHAR types to appropriate size
	if strings.HasPrefix(strings.ToUpper(col.DataType), "VARCHAR") {
		optimized.PgType = c.handleVarcharType(col.DataType)

		// Log significant size reductions
		if strings.Contains(col.DataType, "16777216") && optimized.PgType != "TEXT" {
			c.logger.Info("Optimized oversized VARCHAR",
				zap.String("column", col.Name),
				zap.String("from", col.DataType),
				zap.String("to", optimized.PgType))
		}
	}

	// Optimize date/time fields based on naming conventions
	if isDateTimeColumn(col.Name) && !isDateTimeType(col.DataType) {
		// For specific iCal fields in the SCHEDULE table
		if isICalField(col.Name) {
			optimized.PgType = "TEXT" // Keep as TEXT for iCal formats
		} else if isTimestampField(col.Name) {
			optimized.PgType = "TIMESTAMP WITH TIME ZONE"
			c.logger.Info("Optimized timestamp column",
				zap.String("column", col.Name),
				zap.String("from", col.DataType),
				zap.String("to", optimized.PgType))
		} else {
			optimized.PgType = "TIMESTAMP"
		}
	}

	// Ensure ID columns use UUID type when appropriate
	if isUUIDColumn(col.Name) && !strings.Contains(strings.ToUpper(col.DataType), "UUID") {
		optimized.PgType = "UUID"
	}

	return optimized
}

// Helper functions for column type optimization

func isDateTimeColumn(name string) bool {
	name = strings.ToUpper(name)
	timeFields := []string{
		"DATE", "TIME", "CREATED", "MODIFIED", "UPDATED", "TIMESTAMP",
		"START", "END", "DTSTART", "DTEND", "VALID_FROM", "VALID_TO",
		"LAST_MODIFIED", "CREATED_AT", "UPDATED_AT",
	}

	for _, field := range timeFields {
		if strings.Contains(name, field) {
			return true
		}
	}
	return false
}

func isDateTimeType(dataType string) bool {
	dataType = strings.ToUpper(dataType)
	return strings.Contains(dataType, "TIME") || strings.Contains(dataType, "DATE")
}

func isICalField(name string) bool {
	name = strings.ToUpper(name)
	icalFields := []string{
		"DTSTART", "UNTIL", "FREQ", "WKST", "BYDAY", "BYWEEKNO",
		"BYMONTHDAY", "BYYEARDAY", "OPENS_AT", "CLOSES_AT",
	}

	for _, field := range icalFields {
		if name == field {
			return true
		}
	}
	return false
}

func isTimestampField(name string) bool {
	name = strings.ToUpper(name)
	timestampFields := []string{
		"CREATED", "LAST_MODIFIED", "UPDATED", "CREATED_AT",
		"UPDATED_AT", "MODIFIED_AT", "TIMESTAMP",
	}

	for _, field := range timestampFields {
		if name == field || strings.HasSuffix(name, "_"+field) {
			return true
		}
	}
	return false
}

func isUUIDColumn(name string) bool {
	name = strings.ToUpper(name)
	return name == "ID" || name == "UUID" || strings.HasSuffix(name, "_ID") ||
		strings.HasSuffix(name, "_UUID") || strings.HasPrefix(name, "UUID_")
}

// AnalyzeTableForOptimization examines a table and suggests optimizations
func (c *TypeConverter) AnalyzeTableForOptimization(metadata *model.TableMetadata) []string {
	var suggestions []string

	// Check for oversized VARCHAR columns
	oversizedColumns := 0
	for _, col := range metadata.Columns {
		if strings.Contains(col.DataType, "16777216") {
			oversizedColumns++
		}
	}

	if oversizedColumns > 0 {
		suggestions = append(suggestions,
			fmt.Sprintf("Found %d oversized VARCHAR columns that will be optimized to TEXT", oversizedColumns))
	}

	// Check for potential timestamp columns stored as strings
	stringDateColumns := 0
	for _, col := range metadata.Columns {
		if isDateTimeColumn(col.Name) && !isDateTimeType(col.DataType) {
			stringDateColumns++
		}
	}

	if stringDateColumns > 0 {
		suggestions = append(suggestions,
			fmt.Sprintf("Found %d date/time columns stored as strings that could be optimized", stringDateColumns))
	}

	// Check for array/object columns
	complexColumns := 0
	for _, col := range metadata.Columns {
		if strings.Contains(strings.ToUpper(col.DataType), "ARRAY") ||
			strings.Contains(strings.ToUpper(col.DataType), "OBJECT") {
			complexColumns++
		}
	}

	if complexColumns > 0 {
		suggestions = append(suggestions,
			fmt.Sprintf("Found %d complex type columns (ARRAY/OBJECT) that will be stored as JSONB", complexColumns))
	}

	return suggestions
}
