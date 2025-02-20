// pkg/converter/converter.go
package converter

import (
	"fmt"
	"strings"

	"go.uber.org/zap"

	"github.com/David-Botos/data-ingress/pkg/model"
)

// TypeConverter handles mapping and conversion of data types and values
type TypeConverter struct {
	logger *zap.Logger
	// Configuration options
	config TypeConverterConfig
}

// TypeConverterConfig provides configuration options for type conversion
type TypeConverterConfig struct {
	// Maximum VARCHAR length before converting to TEXT
	MaxVarcharLength int
	// Whether to optimize storage by reducing oversized VARCHARs
	OptimizeStorage bool
	// Default timestamp timezone
	DefaultTimezone string
	// Whether to treat empty strings as NULL
	EmptyStringAsNull bool
	// Preserve original precision for numeric types
	PreserveNumericPrecision bool
}

// DefaultConfig returns the default configuration
func DefaultConfig() TypeConverterConfig {
	return TypeConverterConfig{
		MaxVarcharLength:         10485760, // 10MB is PostgreSQL's TEXT practical limit
		OptimizeStorage:          true,
		DefaultTimezone:          "UTC",
		EmptyStringAsNull:        true,
		PreserveNumericPrecision: false,
	}
}

// NewTypeConverter creates a new TypeConverter with default configuration
func NewTypeConverter(logger *zap.Logger) *TypeConverter {
	return NewTypeConverterWithConfig(logger, DefaultConfig())
}

// NewTypeConverterWithConfig creates a TypeConverter with custom configuration
func NewTypeConverterWithConfig(logger *zap.Logger, config TypeConverterConfig) *TypeConverter {
	return &TypeConverter{
		logger: logger,
		config: config,
	}
}

// MapSnowflakeTypeToPostgres converts a Snowflake data type to PostgreSQL
func (c *TypeConverter) MapSnowflakeTypeToPostgres(snowType string) (string, error) {
	// Handle NULL type
	if snowType == "" || strings.EqualFold(snowType, "NULL") {
		return "TEXT", nil
	}

	// Extract base type and handle special cases
	snowType = strings.ToUpper(snowType)
	baseType := getBaseType(snowType)

	switch baseType {
	case "VARCHAR":
		return c.handleVarcharType(snowType), nil
	case "NUMBER":
		return c.handleNumberType(snowType), nil
	case "ARRAY":
		return "JSONB", nil
	case "OBJECT", "VARIANT":
		return "JSONB", nil
	case "DATE":
		return "DATE", nil
	case "TIMESTAMP_NTZ":
		return "TIMESTAMP", nil
	case "TIMESTAMP_TZ", "TIMESTAMP_LTZ":
		return "TIMESTAMP WITH TIME ZONE", nil
	case "BOOLEAN":
		return "BOOLEAN", nil
	case "FLOAT":
		return "DOUBLE PRECISION", nil
	case "BINARY", "VARBINARY":
		return "BYTEA", nil
	default:
		// Log unexpected type and return error
		c.logger.Warn("Unknown Snowflake type encountered",
			zap.String("snowflakeType", snowType))
		return "TEXT", fmt.Errorf("unknown Snowflake type: %s (mapped to TEXT as fallback)", snowType)
	}
}

// GenerateColumnDefinitions creates PostgreSQL column definitions
func (c *TypeConverter) GenerateColumnDefinitions(metadata *model.TableMetadata) ([]string, error) {
	definitions := make([]string, 0, len(metadata.Columns))

	for _, col := range metadata.Columns {
		pgType := col.PgType
		if pgType == "" {
			pgType, _ = c.MapSnowflakeTypeToPostgres(col.DataType)
		}

		nullability := "NULL"
		if col.IsPrimaryKey || (strings.EqualFold(col.Name, "ID") && !col.Nullable) {
			nullability = "NOT NULL"
		}

		def := fmt.Sprintf("%s %s %s",
			quoteIdentifier(col.Name),
			pgType,
			nullability)

		definitions = append(definitions, def)
	}

	return definitions, nil
}

// quoteIdentifier properly quotes and escapes a PostgreSQL identifier
func quoteIdentifier(name string) string {
	// Handle case sensitivity by quoting lowercase table/column names
	return fmt.Sprintf("\"%s\"", strings.ToLower(strings.ReplaceAll(name, "\"", "\"\"")))
}
