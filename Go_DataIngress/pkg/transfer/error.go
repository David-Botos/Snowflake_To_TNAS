package transfer

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"
)

// Action defines the recommended action after an error
type Action int

const (
	// ActionContinue indicates processing should continue despite the error
	ActionContinue Action = iota
	// ActionRetry indicates the operation should be retried
	ActionRetry
	// ActionSkipRow indicates the current row should be skipped
	ActionSkipRow
	// ActionSkipTable indicates the current table should be skipped
	ActionSkipTable
	// ActionAbort indicates the entire transfer should be aborted
	ActionAbort
)

// ErrorCategory defines categories of errors during transfer
type ErrorCategory int

const (
	// Error categories with increasing severity
	ErrorCategoryNone ErrorCategory = iota
	ErrorCategoryWarning
	ErrorCategoryDataConversion
	ErrorCategoryValidation
	ErrorCategoryRowLevel
	ErrorCategoryChunkLevel
	ErrorCategoryTableLevel
	ErrorCategoryConnectionLevel
	ErrorCategorySystemLevel
	ErrorCategoryCritical
)

// String returns a string representation of the error category
func (ec ErrorCategory) String() string {
	switch ec {
	case ErrorCategoryNone:
		return "None"
	case ErrorCategoryWarning:
		return "Warning"
	case ErrorCategoryDataConversion:
		return "DataConversion"
	case ErrorCategoryValidation:
		return "Validation"
	case ErrorCategoryRowLevel:
		return "RowLevel"
	case ErrorCategoryChunkLevel:
		return "ChunkLevel"
	case ErrorCategoryTableLevel:
		return "TableLevel"
	case ErrorCategoryConnectionLevel:
		return "ConnectionLevel"
	case ErrorCategorySystemLevel:
		return "SystemLevel"
	case ErrorCategoryCritical:
		return "Critical"
	default:
		return fmt.Sprintf("Unknown(%d)", ec)
	}
}

// ErrorRecord represents a single error during transfer
type ErrorRecord struct {
	Category    ErrorCategory
	TableName   string
	RowID       string
	ColumnName  string
	SourceValue interface{}
	Error       error
	Message     string // Derived from Error but stored for serialization
	Timestamp   time.Time
	RetryCount  int
	Recoverable bool
}

// NewErrorRecord creates a new error record with current timestamp
func NewErrorRecord(err error, category ErrorCategory) ErrorRecord {
	record := ErrorRecord{
		Category:    category,
		Error:       err,
		Timestamp:   time.Now(),
		Recoverable: category < ErrorCategoryTableLevel,
	}

	if err != nil {
		record.Message = err.Error()
	}

	return record
}

// WithTable adds table information to the error record
func (r ErrorRecord) WithTable(schema, table string) ErrorRecord {
	r.TableName = fmt.Sprintf("%s.%s", schema, table)
	return r
}

// WithRow adds row information to the error record
func (r ErrorRecord) WithRow(rowID string) ErrorRecord {
	r.RowID = rowID
	return r
}

// WithColumn adds column information to the error record
func (r ErrorRecord) WithColumn(columnName string, sourceValue interface{}) ErrorRecord {
	r.ColumnName = columnName
	r.SourceValue = sourceValue
	return r
}

// WithRetry sets retry information
func (r ErrorRecord) WithRetry(retryCount int) ErrorRecord {
	r.RetryCount = retryCount
	r.Recoverable = r.Category < ErrorCategoryTableLevel && retryCount < 3
	return r
}

// String returns a formatted error message
func (r ErrorRecord) String() string {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("[%s] ", r.Category))

	if r.TableName != "" {
		sb.WriteString(fmt.Sprintf("Table: %s ", r.TableName))
	}

	if r.RowID != "" {
		sb.WriteString(fmt.Sprintf("Row: %s ", r.RowID))
	}

	if r.ColumnName != "" {
		sb.WriteString(fmt.Sprintf("Column: %s ", r.ColumnName))
		if r.SourceValue != nil {
			sb.WriteString(fmt.Sprintf("Value: %v ", r.SourceValue))
		}
	}

	if r.Error != nil {
		sb.WriteString(fmt.Sprintf("Error: %s", r.Error.Error()))
	} else if r.Message != "" {
		sb.WriteString(fmt.Sprintf("Error: %s", r.Message))
	}

	if r.RetryCount > 0 {
		sb.WriteString(fmt.Sprintf(" (Retry: %d)", r.RetryCount))
	}

	return sb.String()
}

// ErrorHandler manages error handling during transfer
type ErrorHandler struct {
	logger          *zap.Logger
	errorThresholds map[ErrorCategory]int
	errorCounts     map[ErrorCategory]int
	sampleErrors    map[ErrorCategory][]ErrorRecord
	tableErrors     map[string]int
	schemaErrors    map[string]int
	mu              sync.Mutex
	maxSamples      int
}

// NewErrorHandler creates a new error handler
func NewErrorHandler(logger *zap.Logger) *ErrorHandler {
	// Default error thresholds by category
	defaultThresholds := map[ErrorCategory]int{
		ErrorCategoryWarning:         1000, // Many warnings are acceptable
		ErrorCategoryDataConversion:  500,  // Quite a few conversion errors acceptable
		ErrorCategoryValidation:      100,  // Some validation errors acceptable
		ErrorCategoryRowLevel:        50,   // Fewer row-level errors acceptable
		ErrorCategoryChunkLevel:      10,   // Very few chunk errors acceptable
		ErrorCategoryTableLevel:      5,    // Minimal table errors acceptable
		ErrorCategoryConnectionLevel: 3,    // Very few connection errors acceptable
		ErrorCategorySystemLevel:     1,    // Almost no system errors acceptable
		ErrorCategoryCritical:        0,    // No critical errors acceptable
	}

	thresholds := make(map[ErrorCategory]int)
	for category, threshold := range defaultThresholds {
		thresholds[category] = threshold
	}

	return &ErrorHandler{
		logger:          logger,
		errorThresholds: thresholds,
		errorCounts:     make(map[ErrorCategory]int),
		sampleErrors:    make(map[ErrorCategory][]ErrorRecord),
		tableErrors:     make(map[string]int),
		schemaErrors:    make(map[string]int),
		maxSamples:      5, // Store up to 5 sample errors per category
	}
}

// CategorizeError determines the category of an error
func (eh *ErrorHandler) CategorizeError(err error) ErrorCategory {
	if err == nil {
		return ErrorCategoryNone
	}

	// Check for specific error types using errors.Is or type assertions
	var category ErrorCategory

	switch {
	// Connection errors
	case strings.Contains(err.Error(), "connection"):
		if strings.Contains(err.Error(), "refused") ||
			strings.Contains(err.Error(), "timeout") ||
			strings.Contains(err.Error(), "EOF") {
			category = ErrorCategoryConnectionLevel
		}

	// Data conversion errors
	case strings.Contains(err.Error(), "convert") ||
		strings.Contains(err.Error(), "parse") ||
		strings.Contains(err.Error(), "unmarshal"):
		category = ErrorCategoryDataConversion

	// Validation errors
	case strings.Contains(err.Error(), "validate") ||
		strings.Contains(err.Error(), "constraint") ||
		strings.Contains(err.Error(), "invalid"):
		category = ErrorCategoryValidation

	// Table-level errors
	case strings.Contains(err.Error(), "table") &&
		(strings.Contains(err.Error(), "not exist") ||
			strings.Contains(err.Error(), "permission") ||
			strings.Contains(err.Error(), "schema")):
		category = ErrorCategoryTableLevel

	// System-level errors
	case strings.Contains(err.Error(), "permission") ||
		strings.Contains(err.Error(), "access") ||
		strings.Contains(err.Error(), "disk") ||
		strings.Contains(err.Error(), "memory"):
		category = ErrorCategorySystemLevel

	// Critical errors
	case strings.Contains(err.Error(), "fatal") ||
		strings.Contains(err.Error(), "panic") ||
		strings.Contains(err.Error(), "terminated"):
		category = ErrorCategoryCritical

	// Default to row-level error if we can't categorize more specifically
	default:
		category = ErrorCategoryRowLevel
	}

	// Log the categorization for debugging
	if eh.logger != nil {
		eh.logger.Debug("Categorized error",
			zap.String("error", err.Error()),
			zap.String("category", category.String()))
	}

	return category
}

// HandleError processes an error and determines action
func (eh *ErrorHandler) HandleError(record ErrorRecord) Action {
	eh.RecordError(record)

	// Determine action based on error category and count
	switch record.Category {
	case ErrorCategoryNone, ErrorCategoryWarning:
		return ActionContinue

	case ErrorCategoryDataConversion, ErrorCategoryValidation, ErrorCategoryRowLevel:
		if record.Recoverable && record.RetryCount < 3 {
			return ActionRetry
		}
		return ActionSkipRow

	case ErrorCategoryChunkLevel:
		if record.Recoverable && record.RetryCount < 2 {
			return ActionRetry
		}
		return ActionSkipTable

	case ErrorCategoryTableLevel:
		return ActionSkipTable

	case ErrorCategoryConnectionLevel:
		if record.RetryCount < 3 {
			// Log connection retry attempt
			if eh.logger != nil {
				eh.logger.Warn("Retrying after connection error",
					zap.String("table", record.TableName),
					zap.Int("retry", record.RetryCount+1),
					zap.String("error", record.Message))
			}
			return ActionRetry
		}
		return ActionSkipTable

	case ErrorCategorySystemLevel, ErrorCategoryCritical:
		// Log critical error
		if eh.logger != nil {
			eh.logger.Error("Critical error during transfer",
				zap.String("category", record.Category.String()),
				zap.String("error", record.Message))
		}
		return ActionAbort

	default:
		return ActionContinue
	}
}

// ShouldAbortTable determines if errors warrant aborting a table transfer
func (eh *ErrorHandler) ShouldAbortTable(tableName string) bool {
	eh.mu.Lock()
	defer eh.mu.Unlock()

	// Get error count for this table
	count, exists := eh.tableErrors[tableName]
	if !exists {
		return false
	}

	// If table errors exceed threshold, abort
	if count >= eh.errorThresholds[ErrorCategoryTableLevel] {
		if eh.logger != nil {
			eh.logger.Warn("Aborting table transfer due to error threshold",
				zap.String("table", tableName),
				zap.Int("errorCount", count),
				zap.Int("threshold", eh.errorThresholds[ErrorCategoryTableLevel]))
		}
		return true
	}

	return false
}

// ShouldAbortSchema determines if errors warrant aborting a schema transfer
func (eh *ErrorHandler) ShouldAbortSchema(schemaName string) bool {
	eh.mu.Lock()
	defer eh.mu.Unlock()

	// Get error count for this schema
	schemaErrorCount, exists := eh.schemaErrors[schemaName]
	if !exists {
		return false
	}

	// Check if critical errors exist (immediate abort condition)
	if eh.errorCounts[ErrorCategoryCritical] > 0 {
		if eh.logger != nil {
			eh.logger.Error("Aborting schema transfer due to critical errors",
				zap.String("schema", schemaName),
				zap.Int("criticalErrors", eh.errorCounts[ErrorCategoryCritical]))
		}
		return true
	}

	// Check if system-level errors exceed threshold
	systemErrors := eh.errorCounts[ErrorCategorySystemLevel]
	if systemErrors >= eh.errorThresholds[ErrorCategorySystemLevel] {
		if eh.logger != nil {
			eh.logger.Error("Aborting schema transfer due to system error threshold",
				zap.String("schema", schemaName),
				zap.Int("errorCount", systemErrors),
				zap.Int("threshold", eh.errorThresholds[ErrorCategorySystemLevel]))
		}
		return true
	}

	// Check if connection-level errors exceed threshold
	connectionErrors := eh.errorCounts[ErrorCategoryConnectionLevel]
	if connectionErrors >= eh.errorThresholds[ErrorCategoryConnectionLevel] {
		if eh.logger != nil {
			eh.logger.Error("Aborting schema transfer due to connection error threshold",
				zap.String("schema", schemaName),
				zap.Int("errorCount", connectionErrors),
				zap.Int("threshold", eh.errorThresholds[ErrorCategoryConnectionLevel]))
		}
		return true
	}

	// Count tables in the schema
	var tablesInSchema []string
	tablesWithErrors := 0
	for tableName := range eh.tableErrors {
		if strings.HasPrefix(tableName, schemaName+".") {
			tablesInSchema = append(tablesInSchema, tableName)
			if eh.tableErrors[tableName] > 0 {
				tablesWithErrors++
			}
		}
	}

	// Check for high error rates across tables
	if len(tablesInSchema) > 0 {
		// If more than 40% of tables have errors, abort
		errorRatio := float64(tablesWithErrors) / float64(len(tablesInSchema))
		if errorRatio > 0.4 {
			if eh.logger != nil {
				eh.logger.Error("Aborting schema transfer due to widespread table errors",
					zap.String("schema", schemaName),
					zap.Int("tablesWithErrors", tablesWithErrors),
					zap.Int("totalTables", len(tablesInSchema)),
					zap.Float64("errorRatio", errorRatio))
			}
			return true
		}

		// Calculate schema error threshold based on table count
		// Use a sliding scale: more tables = higher acceptable error count
		schemaThreshold := eh.calculateSchemaErrorThreshold(len(tablesInSchema))

		if schemaErrorCount >= schemaThreshold {
			if eh.logger != nil {
				eh.logger.Error("Aborting schema transfer due to schema error threshold",
					zap.String("schema", schemaName),
					zap.Int("errorCount", schemaErrorCount),
					zap.Int("threshold", schemaThreshold),
					zap.Int("tableCount", len(tablesInSchema)))
			}
			return true
		}
	}

	// Check if any high-severity categories exceed thresholds
	for _, category := range []ErrorCategory{
		ErrorCategoryTableLevel,
		ErrorCategoryChunkLevel,
	} {
		if count := eh.errorCounts[category]; count >= eh.errorThresholds[category] {
			if eh.logger != nil {
				eh.logger.Error("Aborting schema transfer due to error threshold",
					zap.String("schema", schemaName),
					zap.String("category", category.String()),
					zap.Int("errorCount", count),
					zap.Int("threshold", eh.errorThresholds[category]))
			}
			return true
		}
	}

	return false
}

// calculateSchemaErrorThreshold determines an appropriate error threshold
// based on the number of tables in the schema
func (eh *ErrorHandler) calculateSchemaErrorThreshold(tableCount int) int {
	// Base threshold
	baseThreshold := 5

	if tableCount <= 5 {
		return baseThreshold
	} else if tableCount <= 20 {
		// For small/medium schemas: 5 + 2 per table over 5
		return baseThreshold + 2*(tableCount-5)
	} else if tableCount <= 50 {
		// For medium/large schemas: 35 + 1 per table over 20
		return 35 + (tableCount - 20)
	} else {
		// For very large schemas: 65 + 0.5 per table over 50
		return 65 + (tableCount-50)/2
	}
}

// ShouldRetry determines if operation should be retried
func (eh *ErrorHandler) ShouldRetry(record ErrorRecord) bool {
	// Don't retry if we've already hit the retry limit
	if record.RetryCount >= 3 {
		return false
	}

	// Determine if the error category is retryable
	switch record.Category {
	case ErrorCategoryConnectionLevel:
		// Always retry connection errors up to the limit
		return true

	case ErrorCategoryDataConversion, ErrorCategoryValidation, ErrorCategoryRowLevel:
		// Retry these if they're marked recoverable
		return record.Recoverable

	case ErrorCategoryChunkLevel:
		// Retry chunk errors with a lower threshold
		return record.RetryCount < 2

	case ErrorCategoryTableLevel, ErrorCategorySystemLevel, ErrorCategoryCritical:
		// Never retry these higher-severity errors
		return false

	default:
		return false
	}
}

// RecordError saves an error occurrence
func (eh *ErrorHandler) RecordError(record ErrorRecord) {
	eh.mu.Lock()
	defer eh.mu.Unlock()

	// Increment the category counter
	eh.errorCounts[record.Category]++

	// Save sample errors (up to max samples per category)
	samples := eh.sampleErrors[record.Category]
	if len(samples) < eh.maxSamples {
		eh.sampleErrors[record.Category] = append(samples, record)
	}

	// Track errors by table
	if record.TableName != "" {
		eh.tableErrors[record.TableName]++

		// Extract schema from table name (assuming format "schema.table")
		parts := strings.Split(record.TableName, ".")
		if len(parts) > 0 {
			schemaName := parts[0]
			eh.schemaErrors[schemaName]++
		}
	}

	// Log the error
	if eh.logger != nil {
		logLevel := zap.InfoLevel

		// Use appropriate log level based on error category
		switch record.Category {
		case ErrorCategoryWarning:
			logLevel = zap.WarnLevel
		case ErrorCategoryConnectionLevel, ErrorCategoryTableLevel:
			logLevel = zap.WarnLevel
		case ErrorCategorySystemLevel, ErrorCategoryCritical:
			logLevel = zap.ErrorLevel
		default:
			logLevel = zap.InfoLevel
		}

		eh.logger.Log(logLevel, "Transfer error",
			zap.String("category", record.Category.String()),
			zap.String("table", record.TableName),
			zap.String("error", record.Message),
			zap.Bool("recoverable", record.Recoverable),
			zap.Int("retryCount", record.RetryCount))
	}
}

// GetErrorSummary generates an error summary report
func (eh *ErrorHandler) GetErrorSummary() map[ErrorCategory]int {
	eh.mu.Lock()
	defer eh.mu.Unlock()

	// Create a copy of the current error counts
	summary := make(map[ErrorCategory]int)
	for category, count := range eh.errorCounts {
		summary[category] = count
	}

	return summary
}

// GetErrorSamples returns sample errors for each category
func (eh *ErrorHandler) GetErrorSamples() map[ErrorCategory][]ErrorRecord {
	eh.mu.Lock()
	defer eh.mu.Unlock()

	// Create a deep copy of samples to avoid concurrent modification
	samples := make(map[ErrorCategory][]ErrorRecord)
	for category, records := range eh.sampleErrors {
		categorySamples := make([]ErrorRecord, len(records))
		copy(categorySamples, records)
		samples[category] = categorySamples
	}

	return samples
}

// GetTableErrorCounts returns error counts by table
func (eh *ErrorHandler) GetTableErrorCounts() map[string]int {
	eh.mu.Lock()
	defer eh.mu.Unlock()

	// Create a copy of the table error counts
	tableCounts := make(map[string]int)
	for table, count := range eh.tableErrors {
		tableCounts[table] = count
	}

	return tableCounts
}

// IsErrorThresholdExceeded checks if any error category has exceeded its threshold
func (eh *ErrorHandler) IsErrorThresholdExceeded() bool {
	eh.mu.Lock()
	defer eh.mu.Unlock()

	for category, count := range eh.errorCounts {
		threshold, exists := eh.errorThresholds[category]
		if exists && count > threshold {
			return true
		}
	}

	return false
}

// WrapError creates a new error with additional context
func WrapError(err error, message string) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%s: %w", message, err)
}

// IsRetryableError checks if an error should be retried based on its type/message
func IsRetryableError(err error) bool {
	if err == nil {
		return false
	}

	// Check for specific retryable errors
	errorMsg := err.Error()
	return strings.Contains(errorMsg, "connection") ||
		strings.Contains(errorMsg, "timeout") ||
		strings.Contains(errorMsg, "temporary") ||
		strings.Contains(errorMsg, "deadline") ||
		strings.Contains(errorMsg, "try again") ||
		errors.Is(err, errors.New("context deadline exceeded"))
}
