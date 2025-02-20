// pkg/converter/mapping.go
package converter

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"
)

// Patterns for type extraction
var (
	precisionScalePattern = regexp.MustCompile(`NUMBER\((\d+)(?:,(\d+))?\)`)
	varcharLengthPattern  = regexp.MustCompile(`VARCHAR\((\d+)\)`)
)

// getBaseType extracts the base type from a complex type definition
func getBaseType(fullType string) string {
	parts := strings.Split(fullType, "(")
	return strings.TrimSpace(parts[0])
}

// handleVarcharType optimizes VARCHAR types
func (c *TypeConverter) handleVarcharType(fullType string) string {
	// For massive VARCHARs like VARCHAR(16777216), use TEXT
	if c.config.OptimizeStorage {
		matches := varcharLengthPattern.FindStringSubmatch(fullType)
		if len(matches) > 1 {
			length, err := strconv.Atoi(matches[1])
			if err == nil && length > c.config.MaxVarcharLength {
				c.logger.Debug("Converting large VARCHAR to TEXT",
					zap.String("original", fullType),
					zap.Int("length", length))
				return "TEXT"
			}

			// Handle reasonable VARCHAR sizes
			if length > 10485760 { // PG's practical TEXT limit
				return "TEXT"
			} else if length > 65535 { // Over TEXT type threshold
				return "TEXT"
			} else if length > 10000 {
				return "TEXT"
			} else if length > 1000 {
				return "VARCHAR(1000)" // Reasonable limit for most fields
			} else if length > 255 {
				return "VARCHAR(255)" // Common limit for many string fields
			} else if length > 100 {
				return "VARCHAR(100)"
			} else if length > 50 {
				return "VARCHAR(50)"
			} else {
				// Keep the original size for small fields
				return fmt.Sprintf("VARCHAR(%d)", length)
			}
		}
	}

	// Default to TEXT for unspecified length or non-optimized case
	return "TEXT"
}

// handleNumberType processes NUMBER type with precision/scale
func (c *TypeConverter) handleNumberType(fullType string) string {
	matches := precisionScalePattern.FindStringSubmatch(fullType)

	// No precision/scale specified
	if len(matches) < 2 {
		return "NUMERIC"
	}

	precision, err := strconv.Atoi(matches[1])
	if err != nil {
		return "NUMERIC"
	}

	// Scale defaults to 0 if not specified
	scale := 0
	if len(matches) > 2 && matches[2] != "" {
		scale, err = strconv.Atoi(matches[2])
		if err != nil {
			scale = 0
		}
	}

	// For integers (scale = 0)
	if scale == 0 {
		if precision <= 4 {
			return "SMALLINT"
		} else if precision <= 9 {
			return "INTEGER"
		} else if precision <= 18 {
			return "BIGINT"
		}
		// For larger integers, use NUMERIC with precision
		return fmt.Sprintf("NUMERIC(%d)", precision)
	}

	// For decimals, preserve precision and scale
	if c.config.PreserveNumericPrecision {
		return fmt.Sprintf("NUMERIC(%d,%d)", precision, scale)
	}

	// Default NUMERIC with standard precision based on size
	if precision <= 6 {
		return "NUMERIC(6,2)"
	} else if precision <= 10 {
		return "NUMERIC(10,2)"
	} else if precision <= 15 {
		return "NUMERIC(15,2)"
	} else {
		return "NUMERIC(38,6)" // Maximum precision in Postgres
	}
}

// DetectTimeFormat analyzes a value to determine its timestamp format
func DetectTimeFormat(value string) string {
	// Common formats to check
	formats := []string{
		"2006-01-02T15:04:05Z",             // ISO8601 UTC
		"2006-01-02T15:04:05-07:00",        // ISO8601 with timezone
		"2006-01-02 15:04:05",              // SQL timestamp
		"2006-01-02",                       // Date only
		"15:04:05",                         // Time only
		"20060102T150405Z",                 // Compact ISO8601
		"2006-01-02T15:04:05.999999Z",      // ISO8601 with microseconds
		"2006-01-02T15:04:05.999999-07:00", // ISO8601 with microseconds and TZ
	}

	for _, format := range formats {
		_, err := time.Parse(format, value)
		if err == nil {
			return format
		}
	}

	return ""
}
