// pkg/converter/values.go
package converter

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// ConvertValueForPostgres converts a value to a PostgreSQL compatible type
func (c *TypeConverter) ConvertValueForPostgres(value interface{}, targetType string, colName string) (interface{}, error) {
	// Handle NULL values
	if isNull(value) {
		return nil, nil
	}

	// Special handling based on target PostgreSQL type
	targetType = strings.ToLower(targetType)

	switch {
	case strings.HasPrefix(targetType, "varchar"), targetType == "text":
		return c.convertToText(value)

	case strings.HasPrefix(targetType, "numeric"),
		strings.HasPrefix(targetType, "decimal"),
		targetType == "integer",
		targetType == "smallint",
		targetType == "bigint",
		targetType == "double precision",
		targetType == "real":
		return c.convertToNumeric(value, targetType)

	case targetType == "boolean":
		return c.convertToBoolean(value)

	case strings.Contains(targetType, "timestamp"), targetType == "date":
		return c.convertToTimestamp(value, targetType)

	case targetType == "jsonb", targetType == "json":
		return c.convertToJSON(value)

	default:
		// Default to string conversion for unknown types
		strVal, err := c.convertToText(value)
		if err != nil {
			return nil, fmt.Errorf("fallback string conversion failed for %s: %w", colName, err)
		}
		return strVal, nil
	}
}

// isNull determines if a value should be treated as NULL
func isNull(value interface{}) bool {
	if value == nil {
		return true
	}

	// Check string representations of NULL
	if strVal, ok := value.(string); ok {
		nullValues := []string{"null", "NULL", "nil", "NIL", ""}
		for _, null := range nullValues {
			if strVal == null {
				return true
			}
		}
	}

	return false
}

// convertToText converts a value to text/string
func (c *TypeConverter) convertToText(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		if v == "" && c.config.EmptyStringAsNull {
			return "", nil
		}
		return v, nil
	case []byte:
		return string(v), nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64, bool:
		return fmt.Sprintf("%v", v), nil
	case time.Time:
		return v.Format(time.RFC3339), nil
	case nil:
		return "", nil
	default:
		// Try JSON marshaling for complex types
		jsonBytes, err := json.Marshal(v)
		if err != nil {
			return fmt.Sprintf("%v", v), nil
		}
		return string(jsonBytes), nil
	}
}

// convertToNumeric converts a value to a numeric type
func (c *TypeConverter) convertToNumeric(value interface{}, targetType string) (interface{}, error) {
	switch v := value.(type) {
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		// Already numeric, no conversion needed for database/sql
		return v, nil
	case string:
		if v == "" {
			return nil, nil
		}

		// Try to parse as float first
		if floatVal, err := strconv.ParseFloat(v, 64); err == nil {
			// For integer types, convert to appropriate int type
			if strings.Contains(targetType, "int") {
				return int64(floatVal), nil
			}
			return floatVal, nil
		}

		return nil, fmt.Errorf("cannot convert string '%s' to numeric", v)
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to numeric", value)
	}
}

// convertToBoolean converts a value to boolean
func (c *TypeConverter) convertToBoolean(value interface{}) (interface{}, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		// 0 is false, anything else is true
		return v != 0, nil
	case float32, float64:
		// 0.0 is false, anything else is true
		return v != 0.0, nil
	case string:
		v = strings.ToLower(strings.TrimSpace(v))
		switch v {
		case "true", "t", "yes", "y", "1", "on":
			return true, nil
		case "false", "f", "no", "n", "0", "off", "":
			return false, nil
		default:
			return nil, fmt.Errorf("cannot convert string '%s' to boolean", v)
		}
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to boolean", value)
	}
}

// convertToTimestamp converts a value to timestamp
func (c *TypeConverter) convertToTimestamp(value interface{}, targetType string) (interface{}, error) {
	switch v := value.(type) {
	case time.Time:
		return v, nil
	case string:
		if v == "" {
			return nil, nil
		}

		// Detect format (if possible)
		format := DetectTimeFormat(v)
		if format != "" {
			parsedTime, err := time.Parse(format, v)
			if err == nil {
				return parsedTime, nil
			}
		}

		// Try common formats
		layouts := []string{
			time.RFC3339,
			"2006-01-02T15:04:05Z07:00",
			"2006-01-02 15:04:05",
			"2006-01-02",
			time.RFC1123,
			time.RFC1123Z,
		}

		for _, layout := range layouts {
			parsedTime, err := time.Parse(layout, v)
			if err == nil {
				return parsedTime, nil
			}
		}

		// For iCal format dates in the SCHEDULE table
		if strings.Contains(v, "DTSTART:") || strings.Contains(v, "DTEND:") {
			// Extract the date portion from iCal format
			dateParts := strings.Split(v, ":")
			if len(dateParts) > 1 {
				dateStr := dateParts[1]
				// Handle common iCal date formats like 20210101T120000Z
				if len(dateStr) >= 8 {
					year := dateStr[0:4]
					month := dateStr[4:6]
					day := dateStr[6:8]
					formattedDate := fmt.Sprintf("%s-%s-%s", year, month, day)

					// If time component exists
					if len(dateStr) >= 15 && strings.Contains(dateStr, "T") {
						timeParts := strings.Split(dateStr, "T")
						if len(timeParts) > 1 {
							timeStr := timeParts[1]
							hour := timeStr[0:2]
							minute := timeStr[2:4]
							second := timeStr[4:6]

							formattedDate = fmt.Sprintf("%s %s:%s:%s", formattedDate, hour, minute, second)
							if strings.HasSuffix(timeStr, "Z") {
								formattedDate += " UTC"
							}
						}
					}

					// Try to parse the formatted date
					parsedTime, err := time.Parse("2006-01-02 15:04:05 MST", formattedDate)
					if err == nil {
						return parsedTime, nil
					}

					parsedTime, err = time.Parse("2006-01-02", formattedDate)
					if err == nil {
						return parsedTime, nil
					}
				}
			}
		}

		return nil, fmt.Errorf("cannot parse '%s' as timestamp", v)

	case int64:
		// Assume Unix timestamp (seconds since epoch)
		return time.Unix(v, 0), nil
	case float64:
		// Assume Unix timestamp with possible fractional seconds
		sec := int64(v)
		nsec := int64((v - float64(sec)) * 1e9)
		return time.Unix(sec, nsec), nil
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("cannot convert %T to timestamp", value)
	}
}
