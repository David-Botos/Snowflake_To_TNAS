// pkg/cleaner/operations.go
package cleaner

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/David-Botos/data-ingress/pkg/model"
)

// ensureValidID ensures the ID column contains a valid UUID
// Always returns a valid UUID string, generating a new one if necessary
func ensureValidID(
	value interface{},
	exists bool,
	schema, table string,
) (interface{}, *model.CleaningOperation) {
	// If ID doesn't exist or is nil, generate a new UUID
	if !exists || value == nil {
		newID := uuid.New().String()
		return newID, &model.CleaningOperation{
			SchemaName:        schema,
			TableName:         table,
			ColumnName:        "ID",
			OriginalValue:     nil,
			NewValue:          newID,
			RowIdentifier:     newID, // ID is the row identifier
			CleaningOperation: "uuid_generation",
			CleaningReason:    "missing_primary_key",
		}
	}

	// Convert value to string
	strValue := toString(value)

	// Generate new UUID if empty string
	if strValue == "" {
		newID := uuid.New().String()
		return newID, &model.CleaningOperation{
			SchemaName:        schema,
			TableName:         table,
			ColumnName:        "ID",
			OriginalValue:     strValue,
			NewValue:          newID,
			RowIdentifier:     newID,
			CleaningOperation: "uuid_generation",
			CleaningReason:    "empty_primary_key",
		}
	}

	// Validate UUID format
	if !isValidUUID(strValue) {
		newID := uuid.New().String()
		return newID, &model.CleaningOperation{
			SchemaName:        schema,
			TableName:         table,
			ColumnName:        "ID",
			OriginalValue:     strValue,
			NewValue:          newID,
			RowIdentifier:     newID,
			CleaningOperation: "uuid_generation",
			CleaningReason:    "invalid_primary_key",
		}
	}

	// Already valid UUID
	return strValue, nil
}

// cleanUUIDValue ensures a UUID column contains a valid UUID
func cleanUUIDValue(
	value interface{},
	colName string,
	rowID string,
	schema, table string,
) (interface{}, *model.CleaningOperation) {
	// For null values in non-ID UUID columns, keep them null
	// This aligns with requirement #3 to preserve nulls in non-critical fields
	if value == nil && colName != "ID" {
		return nil, nil
	}

	// Convert to string
	strValue := toString(value)
	if strValue == "" && colName != "ID" {
		// For empty strings in non-ID UUID columns, consider them as nulls
		return nil, nil
	}

	// For non-empty values, validate UUID format
	if strValue != "" && !isValidUUID(strValue) {
		newUUID := uuid.New().String()
		return newUUID, &model.CleaningOperation{
			SchemaName:        schema,
			TableName:         table,
			ColumnName:        colName,
			OriginalValue:     strValue,
			NewValue:          newUUID,
			RowIdentifier:     rowID,
			CleaningOperation: "uuid_generation",
			CleaningReason:    "invalid_uuid_format",
		}
	}

	// Already valid UUID or appropriately null
	return value, nil
}

// standardizeDataType attempts to standardize a value to match expected data type
// Preserves nulls for non-critical fields
func standardizeDataType(
	value interface{},
	colMeta *model.Column,
	rowID string,
	schema, table string,
) (interface{}, *model.CleaningOperation) {
	// Preserve nulls for non-critical fields (requirement #3)
	if value == nil && colMeta.Name != "ID" {
		return nil, nil
	}

	// Skip empty values for non-critical fields
	strValue := toString(value)
	if strValue == "" && colMeta.Name != "ID" {
		return nil, nil
	}

	// Handle different data types
	switch strings.ToUpper(colMeta.DataType) {
	case "INT", "INTEGER", "BIGINT", "SMALLINT":
		return standardizeInteger(value, colMeta, rowID, schema, table)
	case "FLOAT", "REAL", "DOUBLE", "NUMERIC", "DECIMAL":
		return standardizeFloat(value, colMeta, rowID, schema, table)
	case "BOOL", "BOOLEAN":
		return standardizeBoolean(value, colMeta, rowID, schema, table)
	case "DATE", "TIMESTAMP", "TIMESTAMPTZ", "DATETIME":
		return standardizeDateTime(value, colMeta, rowID, schema, table)
	default:
		// For other types, just ensure it's a properly formatted string
		return value, nil
	}
}

// validateDataType checks if a value matches the expected data type
// Returns error if invalid, nil if valid
func validateDataType(value interface{}, dataType string) error {
	if value == nil {
		return nil // Nulls are allowed (except for ID, which is handled separately)
	}

	switch strings.ToUpper(dataType) {
	case "INT", "INTEGER", "BIGINT", "SMALLINT":
		_, err := toInt(value)
		if err != nil {
			return fmt.Errorf("expected integer, got %T: %w", value, err)
		}
	case "FLOAT", "REAL", "DOUBLE", "NUMERIC", "DECIMAL":
		_, err := toFloat(value)
		if err != nil {
			return fmt.Errorf("expected float, got %T: %w", value, err)
		}
	case "BOOL", "BOOLEAN":
		_, err := toBool(value)
		if err != nil {
			return fmt.Errorf("expected boolean, got %T: %w", value, err)
		}
	case "DATE", "TIMESTAMP", "TIMESTAMPTZ", "DATETIME":
		_, err := toTime(value)
		if err != nil {
			return fmt.Errorf("expected date/time, got %T: %w", value, err)
		}
	case "UUID":
		strVal := toString(value)
		if !isValidUUID(strVal) {
			return errors.New("invalid UUID format")
		}
	}
	return nil
}

// Helper functions

// isUUIDColumn determines if a column should be treated as a UUID
func isUUIDColumn(colName string) bool {
	colName = strings.ToUpper(colName)
	return colName == "UUID" || colName == "ID" ||
		strings.HasSuffix(colName, "_UUID") || strings.HasSuffix(colName, "_ID")
}

// isValidUUID checks if a string is a valid UUID
func isValidUUID(u string) bool {
	_, err := uuid.Parse(u)
	return err == nil
}

// toString converts an interface to string
func toString(v interface{}) string {
	if v == nil {
		return ""
	}

	switch val := v.(type) {
	case string:
		return val
	case []byte:
		return string(val)
	default:
		// Use Sprint as a fallback
		return fmt.Sprintf("%v", val)
	}
}

// toNullableString safely converts an interface to a nullable string
func toNullableString(v interface{}) *string {
	if v == nil {
		return nil
	}
	s := toString(v)
	return &s
}

// toInt attempts to convert a value to int64
func toInt(v interface{}) (int64, error) {
	if v == nil {
		return 0, errors.New("nil value")
	}

	switch val := v.(type) {
	case int:
		return int64(val), nil
	case int8:
		return int64(val), nil
	case int16:
		return int64(val), nil
	case int32:
		return int64(val), nil
	case int64:
		return val, nil
	case uint:
		return int64(val), nil
	case uint8:
		return int64(val), nil
	case uint16:
		return int64(val), nil
	case uint32:
		return int64(val), nil
	case uint64:
		if val > uint64(9223372036854775807) {
			return 0, errors.New("uint64 value overflow for int64")
		}
		return int64(val), nil
	case float32:
		return int64(val), nil
	case float64:
		return int64(val), nil
	case string:
		cleaned := strings.TrimSpace(val)
		if cleaned == "" {
			return 0, errors.New("empty string")
		}
		return strconv.ParseInt(cleaned, 10, 64)
	case []byte:
		cleaned := strings.TrimSpace(string(val))
		if cleaned == "" {
			return 0, errors.New("empty byte array")
		}
		return strconv.ParseInt(cleaned, 10, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to int", v)
	}
}

// toFloat attempts to convert a value to float64
func toFloat(v interface{}) (float64, error) {
	if v == nil {
		return 0, errors.New("nil value")
	}

	switch val := v.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		// Use toString to avoid type assertion for each numeric type
		str := toString(val)
		return strconv.ParseFloat(str, 64)
	case float32:
		return float64(val), nil
	case float64:
		return val, nil
	case string:
		cleaned := strings.TrimSpace(val)
		if cleaned == "" {
			return 0, errors.New("empty string")
		}
		return strconv.ParseFloat(cleaned, 64)
	case []byte:
		cleaned := strings.TrimSpace(string(val))
		if cleaned == "" {
			return 0, errors.New("empty byte array")
		}
		return strconv.ParseFloat(cleaned, 64)
	default:
		return 0, fmt.Errorf("cannot convert %T to float", v)
	}
}

// toBool attempts to convert a value to bool
func toBool(v interface{}) (bool, error) {
	if v == nil {
		return false, errors.New("nil value")
	}

	switch val := v.(type) {
	case bool:
		return val, nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		// Convert numeric values (0 = false, non-0 = true)
		i, _ := toInt(val)
		return i != 0, nil
	case string:
		cleaned := strings.TrimSpace(strings.ToLower(val))
		switch cleaned {
		case "true", "t", "yes", "y", "1":
			return true, nil
		case "false", "f", "no", "n", "0":
			return false, nil
		default:
			return false, fmt.Errorf("cannot parse '%s' as boolean", val)
		}
	default:
		return false, fmt.Errorf("cannot convert %T to bool", v)
	}
}

// toTime attempts to convert a value to time.Time
func toTime(v interface{}) (time.Time, error) {
	if v == nil {
		return time.Time{}, errors.New("nil value")
	}

	switch val := v.(type) {
	case time.Time:
		return val, nil
	case string:
		cleaned := strings.TrimSpace(val)
		if cleaned == "" {
			return time.Time{}, errors.New("empty string")
		}

		// Try common formats
		formats := []string{
			time.RFC3339,
			"2006-01-02T15:04:05",
			"2006-01-02 15:04:05",
			"2006-01-02",
			"01/02/2006",
			"01-02-2006",
			"2006/01/02",
		}

		for _, format := range formats {
			if t, err := time.Parse(format, cleaned); err == nil {
				return t, nil
			}
		}

		return time.Time{}, fmt.Errorf("cannot parse time from '%s'", cleaned)
	default:
		return time.Time{}, fmt.Errorf("cannot convert %T to time", v)
	}
}

// standardizeInteger converts value to int64 if possible, otherwise keeps original
func standardizeInteger(
	value interface{},
	colMeta *model.Column,
	rowID string,
	schema, table string,
) (interface{}, *model.CleaningOperation) {
	intVal, err := toInt(value)
	if err != nil {
		// If conversion fails, keep original value and log operation
		return value, &model.CleaningOperation{
			SchemaName:        schema,
			TableName:         table,
			ColumnName:        colMeta.Name,
			OriginalValue:     toString(value),
			NewValue:          toString(value), // Same as original, indicating no change
			RowIdentifier:     rowID,
			CleaningOperation: "type_validation_failed",
			CleaningReason:    fmt.Sprintf("cannot_convert_to_int: %v", err),
		}
	}

	// Successfully converted to int
	// Return value as is if it was already correct type
	if _, ok := value.(int64); ok {
		return value, nil
	}

	return intVal, &model.CleaningOperation{
		SchemaName:        schema,
		TableName:         table,
		ColumnName:        colMeta.Name,
		OriginalValue:     toString(value),
		NewValue:          toString(intVal),
		RowIdentifier:     rowID,
		CleaningOperation: "type_standardization",
		CleaningReason:    "converted_to_int",
	}
}

// standardizeFloat converts value to float64 if possible
func standardizeFloat(
	value interface{},
	colMeta *model.Column,
	rowID string,
	schema, table string,
) (interface{}, *model.CleaningOperation) {
	floatVal, err := toFloat(value)
	if err != nil {
		// If conversion fails, keep original value and log operation
		return value, &model.CleaningOperation{
			SchemaName:        schema,
			TableName:         table,
			ColumnName:        colMeta.Name,
			OriginalValue:     toString(value),
			NewValue:          toString(value),
			RowIdentifier:     rowID,
			CleaningOperation: "type_validation_failed",
			CleaningReason:    fmt.Sprintf("cannot_convert_to_float: %v", err),
		}
	}

	// Successfully converted to float
	// Return value as is if it was already correct type
	if _, ok := value.(float64); ok {
		return value, nil
	}

	return floatVal, &model.CleaningOperation{
		SchemaName:        schema,
		TableName:         table,
		ColumnName:        colMeta.Name,
		OriginalValue:     toString(value),
		NewValue:          toString(floatVal),
		RowIdentifier:     rowID,
		CleaningOperation: "type_standardization",
		CleaningReason:    "converted_to_float",
	}
}

// standardizeBoolean converts value to bool if possible
func standardizeBoolean(
	value interface{},
	colMeta *model.Column,
	rowID string,
	schema, table string,
) (interface{}, *model.CleaningOperation) {
	boolVal, err := toBool(value)
	if err != nil {
		// If conversion fails, keep original value and log operation
		return value, &model.CleaningOperation{
			SchemaName:        schema,
			TableName:         table,
			ColumnName:        colMeta.Name,
			OriginalValue:     toString(value),
			NewValue:          toString(value),
			RowIdentifier:     rowID,
			CleaningOperation: "type_validation_failed",
			CleaningReason:    fmt.Sprintf("cannot_convert_to_bool: %v", err),
		}
	}

	// Successfully converted to bool
	// Return value as is if it was already correct type
	if _, ok := value.(bool); ok {
		return value, nil
	}

	return boolVal, &model.CleaningOperation{
		SchemaName:        schema,
		TableName:         table,
		ColumnName:        colMeta.Name,
		OriginalValue:     toString(value),
		NewValue:          toString(boolVal),
		RowIdentifier:     rowID,
		CleaningOperation: "type_standardization",
		CleaningReason:    "converted_to_bool",
	}
}

// standardizeDateTime converts value to time.Time if possible
func standardizeDateTime(
	value interface{},
	colMeta *model.Column,
	rowID string,
	schema, table string,
) (interface{}, *model.CleaningOperation) {
	timeVal, err := toTime(value)
	if err != nil {
		// If conversion fails, keep original value and log operation
		return value, &model.CleaningOperation{
			SchemaName:        schema,
			TableName:         table,
			ColumnName:        colMeta.Name,
			OriginalValue:     toString(value),
			NewValue:          toString(value),
			RowIdentifier:     rowID,
			CleaningOperation: "type_validation_failed",
			CleaningReason:    fmt.Sprintf("cannot_convert_to_datetime: %v", err),
		}
	}

	// Successfully converted to time.Time
	// Return value as is if it was already correct type
	if _, ok := value.(time.Time); ok {
		return value, nil
	}

	return timeVal, &model.CleaningOperation{
		SchemaName:        schema,
		TableName:         table,
		ColumnName:        colMeta.Name,
		OriginalValue:     toString(value),
		NewValue:          timeVal.Format(time.RFC3339),
		RowIdentifier:     rowID,
		CleaningOperation: "type_standardization",
		CleaningReason:    "converted_to_datetime",
	}
}
