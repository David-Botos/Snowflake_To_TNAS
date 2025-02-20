package transfer

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/David-Botos/data-ingress/pkg/connector"
	"github.com/David-Botos/data-ingress/pkg/model"
)

// RowDiscrepancy represents a discrepancy between source and target rows
type RowDiscrepancy struct {
	RowID       string
	ColumnName  string
	SourceValue interface{}
	TargetValue interface{}
	Discrepancy string
}

// StructureDiscrepancy represents a discrepancy in table structure
type StructureDiscrepancy struct {
	ColumnName       string
	ExpectedType     string
	ActualType       string
	ExpectedNullable bool
	ActualNullable   bool
	IsMissing        bool
}

// IntegrityIssue represents a data integrity issue
type IntegrityIssue struct {
	IssueType    string
	Description  string
	ColumnName   string
	AffectedRows int64
}

// VerificationReport contains the results of a table verification
type VerificationReport struct {
	Schema                 string
	Table                  string
	VerificationTime       time.Time
	RowCountMatches        bool
	SourceRowCount         int64
	TargetRowCount         int64
	StructureMatches       bool
	StructureDiscrepancies []StructureDiscrepancy
	SampleVerified         bool
	SampleSize             int
	SampleDiscrepancies    []RowDiscrepancy
	IntegrityVerified      bool
	IntegrityIssues        []IntegrityIssue
	Duration               time.Duration
}

// Verifier provides data verification utilities
type Verifier struct {
	snowflake connector.DatabaseConnector
	postgres  connector.DatabaseConnector
	logger    *zap.Logger
	timeout   time.Duration
}

// NewVerifier creates a new verifier
func NewVerifier(
	snowflake connector.DatabaseConnector,
	postgres connector.DatabaseConnector,
	logger *zap.Logger,
) *Verifier {
	return &Verifier{
		snowflake: snowflake,
		postgres:  postgres,
		logger:    logger,
		timeout:   time.Minute * 5, // Default 5-minute timeout
	}
}

// WithTimeout sets a custom timeout for verification operations
func (v *Verifier) WithTimeout(timeout time.Duration) *Verifier {
	v.timeout = timeout
	return v
}

// VerifyRowCount verifies row counts match between source and target
func (v *Verifier) VerifyRowCount(
	ctx context.Context,
	schema, table string,
) (bool, int64, int64, error) {
	v.logger.Info("Verifying row count",
		zap.String("schema", schema),
		zap.String("table", table))

	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, v.timeout)
	defer cancel()

	// Query to count rows
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", schema, table)

	// Get source row count
	var sourceCount int64
	sourceRows, err := v.snowflake.QueryWithTimeout(ctx, countQuery, v.timeout)
	if err != nil {
		return false, 0, 0, fmt.Errorf("failed to query source: %w", err)
	}
	defer sourceRows.Close()

	if sourceRows.Next() {
		if err := sourceRows.Scan(&sourceCount); err != nil {
			return false, 0, 0, fmt.Errorf("failed to scan source count: %w", err)
		}
	} else {
		return false, 0, 0, fmt.Errorf("no results returned from source count query")
	}

	// Get target row count
	var targetCount int64
	targetRows, err := v.postgres.QueryWithTimeout(ctx, countQuery, v.timeout)
	if err != nil {
		return false, 0, 0, fmt.Errorf("failed to query target: %w", err)
	}
	defer targetRows.Close()

	if targetRows.Next() {
		if err := targetRows.Scan(&targetCount); err != nil {
			return false, 0, 0, fmt.Errorf("failed to scan target count: %w", err)
		}
	} else {
		return false, 0, 0, fmt.Errorf("no results returned from target count query")
	}

	// Log the result
	matches := sourceCount == targetCount
	if matches {
		v.logger.Info("Row count verification successful",
			zap.String("schema", schema),
			zap.String("table", table),
			zap.Int64("count", sourceCount))
	} else {
		v.logger.Warn("Row count mismatch",
			zap.String("schema", schema),
			zap.String("table", table),
			zap.Int64("sourceCount", sourceCount),
			zap.Int64("targetCount", targetCount),
			zap.Int64("difference", sourceCount-targetCount))
	}

	return matches, sourceCount, targetCount, nil
}

// VerifySampleRows verifies sample rows between source and target
func (v *Verifier) VerifySampleRows(
	ctx context.Context,
	schema, table string,
	sampleSize int,
) (bool, []RowDiscrepancy, error) {
	v.logger.Info("Verifying sample rows",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Int("sampleSize", sampleSize))

	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, v.timeout)
	defer cancel()

	// Get table metadata to identify columns and primary keys
	metadata, err := v.getTableMetadata(ctx, schema, table)
	if err != nil {
		return false, nil, fmt.Errorf("failed to get table metadata: %w", err)
	}

	// Determine how to sample rows
	samplingStrategy, keyColumns := v.determineSamplingStrategy(metadata)

	// Get sample rows from source
	sourceRows, err := v.getSampleRows(ctx, v.snowflake, schema, table,
		metadata.Columns, samplingStrategy, keyColumns, sampleSize)
	if err != nil {
		return false, nil, fmt.Errorf("failed to get source sample rows: %w", err)
	}

	if len(sourceRows) == 0 {
		v.logger.Warn("No sample rows found in source",
			zap.String("schema", schema),
			zap.String("table", table))
		return true, nil, nil
	}

	// Build a query to fetch the same rows from target
	keyValues := make([]string, 0, len(sourceRows))
	for _, row := range sourceRows {
		keyValues = append(keyValues, v.formatKeyCondition(keyColumns, row))
	}

	// Get matching rows from target
	targetRows, err := v.getMatchingRows(ctx, v.postgres, schema, table,
		metadata.Columns, keyColumns, keyValues)
	if err != nil {
		return false, nil, fmt.Errorf("failed to get target matching rows: %w", err)
	}

	// Compare rows and collect discrepancies
	discrepancies := v.compareRows(sourceRows, targetRows, metadata.Columns, keyColumns)

	// Log results
	matches := len(discrepancies) == 0
	if matches {
		v.logger.Info("Sample row verification successful",
			zap.String("schema", schema),
			zap.String("table", table),
			zap.Int("sampleSize", len(sourceRows)))
	} else {
		v.logger.Warn("Sample row discrepancies found",
			zap.String("schema", schema),
			zap.String("table", table),
			zap.Int("discrepancies", len(discrepancies)))
	}

	return matches, discrepancies, nil
}

// VerifyTableStructure verifies table structure matches metadata
func (v *Verifier) VerifyTableStructure(
	ctx context.Context,
	schema, table string,
	expectedMetadata *model.TableMetadata,
) (bool, []StructureDiscrepancy, error) {
	v.logger.Info("Verifying table structure",
		zap.String("schema", schema),
		zap.String("table", table))

	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, v.timeout)
	defer cancel()

	// Get actual table structure from Postgres
	actualColumns, err := v.getTableColumns(ctx, v.postgres, schema, table)
	if err != nil {
		return false, nil, fmt.Errorf("failed to get table structure: %w", err)
	}

	// Compare expected vs actual structure
	discrepancies := make([]StructureDiscrepancy, 0)

	// Check for missing or different columns
	expectedColumnMap := make(map[string]model.Column)
	for _, col := range expectedMetadata.Columns {
		expectedColumnMap[strings.ToLower(col.Name)] = col
	}

	actualColumnMap := make(map[string]struct {
		DataType string
		Nullable bool
	})

	for _, col := range actualColumns {
		actualColumnMap[strings.ToLower(col.Name)] = struct {
			DataType string
			Nullable bool
		}{
			DataType: col.DataType,
			Nullable: col.Nullable,
		}
	}

	// Check expected columns against actual
	for name, expected := range expectedColumnMap {
		actual, exists := actualColumnMap[name]
		if !exists {
			// Column is missing in target
			discrepancies = append(discrepancies, StructureDiscrepancy{
				ColumnName:   expected.Name,
				ExpectedType: expected.PgType,
				IsMissing:    true,
			})
			continue
		}

		// Check data type matches
		if !strings.EqualFold(expected.PgType, actual.DataType) {
			discrepancies = append(discrepancies, StructureDiscrepancy{
				ColumnName:   expected.Name,
				ExpectedType: expected.PgType,
				ActualType:   actual.DataType,
				IsMissing:    false,
			})
		}

		// Check nullability
		if expected.Nullable != actual.Nullable {
			discrepancies = append(discrepancies, StructureDiscrepancy{
				ColumnName:       expected.Name,
				ExpectedNullable: expected.Nullable,
				ActualNullable:   actual.Nullable,
				IsMissing:        false,
			})
		}
	}

	// Check for extra columns in target (not in expected metadata)
	for name := range actualColumnMap {
		if _, exists := expectedColumnMap[name]; !exists {
			// Extra column in target
			actual := actualColumnMap[name]
			discrepancies = append(discrepancies, StructureDiscrepancy{
				ColumnName: name,
				ActualType: actual.DataType,
				IsMissing:  false,
			})
		}
	}

	// Log results
	matches := len(discrepancies) == 0
	if matches {
		v.logger.Info("Table structure verification successful",
			zap.String("schema", schema),
			zap.String("table", table))
	} else {
		v.logger.Warn("Table structure discrepancies found",
			zap.String("schema", schema),
			zap.String("table", table),
			zap.Int("discrepancies", len(discrepancies)))
	}

	return matches, discrepancies, nil
}

// VerifyDataIntegrity performs data integrity checks
func (v *Verifier) VerifyDataIntegrity(
	ctx context.Context,
	schema, table string,
) (bool, []IntegrityIssue, error) {
	v.logger.Info("Verifying data integrity",
		zap.String("schema", schema),
		zap.String("table", table))

	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, v.timeout)
	defer cancel()

	// Get table metadata
	metadata, err := v.getTableMetadata(ctx, schema, table)
	if err != nil {
		return false, nil, fmt.Errorf("failed to get table metadata: %w", err)
	}

	issues := make([]IntegrityIssue, 0)

	// Check for null values in non-nullable columns
	nullIssues, err := v.checkNullConstraints(ctx, schema, table, metadata)
	if err != nil {
		return false, nil, fmt.Errorf("failed to check null constraints: %w", err)
	}
	issues = append(issues, nullIssues...)

	// Check primary key uniqueness
	if len(metadata.PrimaryKeys) > 0 {
		pkIssues, err := v.checkPrimaryKeyUniqueness(ctx, schema, table, metadata.PrimaryKeys)
		if err != nil {
			return false, nil, fmt.Errorf("failed to check primary key uniqueness: %w", err)
		}
		issues = append(issues, pkIssues...)
	}

	// Log results
	success := len(issues) == 0
	if success {
		v.logger.Info("Data integrity verification successful",
			zap.String("schema", schema),
			zap.String("table", table))
	} else {
		v.logger.Warn("Data integrity issues found",
			zap.String("schema", schema),
			zap.String("table", table),
			zap.Int("issues", len(issues)))
	}

	return success, issues, nil
}

// GenerateVerificationReport creates a verification report
func (v *Verifier) GenerateVerificationReport(
	ctx context.Context,
	schema, table string,
) (*VerificationReport, error) {
	v.logger.Info("Generating verification report",
		zap.String("schema", schema),
		zap.String("table", table))

	startTime := time.Now()
	report := &VerificationReport{
		Schema:           schema,
		Table:            table,
		VerificationTime: startTime,
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(ctx, v.timeout)
	defer cancel()

	// Get table metadata
	metadata, err := v.getTableMetadata(ctx, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %w", err)
	}

	// 1. Verify row count
	rowCountMatch, sourceCount, targetCount, err := v.VerifyRowCount(ctx, schema, table)
	if err != nil {
		v.logger.Warn("Row count verification failed",
			zap.String("schema", schema),
			zap.String("table", table),
			zap.Error(err))
	} else {
		report.RowCountMatches = rowCountMatch
		report.SourceRowCount = sourceCount
		report.TargetRowCount = targetCount
	}

	// 2. Verify table structure
	structureMatch, discrepancies, err := v.VerifyTableStructure(ctx, schema, table, metadata)
	if err != nil {
		v.logger.Warn("Structure verification failed",
			zap.String("schema", schema),
			zap.String("table", table),
			zap.Error(err))
	} else {
		report.StructureMatches = structureMatch
		report.StructureDiscrepancies = discrepancies
	}

	// 3. Verify sample rows (if counts match)
	if rowCountMatch && structureMatch {
		sampleSize := calculateSampleSize(sourceCount)
		sampleMatch, discrepancies, err := v.VerifySampleRows(ctx, schema, table, sampleSize)
		if err != nil {
			v.logger.Warn("Sample verification failed",
				zap.String("schema", schema),
				zap.String("table", table),
				zap.Error(err))
		} else {
			report.SampleVerified = true
			report.SampleSize = sampleSize
			report.SampleDiscrepancies = discrepancies
			// Store sample match result in the report
			report.SampleVerified = sampleMatch
		}
	}

	// 4. Verify data integrity
	integrityMatch, issues, err := v.VerifyDataIntegrity(ctx, schema, table)
	if err != nil {
		v.logger.Warn("Integrity verification failed",
			zap.String("schema", schema),
			zap.String("table", table),
			zap.Error(err))
	} else {
		report.IntegrityVerified = true
		report.IntegrityIssues = issues
		// Update report to reflect integrity check results
		report.IntegrityVerified = integrityMatch
	}

	// Calculate duration
	report.Duration = time.Since(startTime)

	v.logger.Info("Verification report completed",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Duration("duration", report.Duration),
		zap.Bool("rowCountMatch", report.RowCountMatches),
		zap.Bool("structureMatch", report.StructureMatches))

	return report, nil
}

// Helper functions

// checkNullConstraints verifies that non-nullable columns don't contain NULL values
func (v *Verifier) checkNullConstraints(
	ctx context.Context,
	schema, table string,
	metadata *model.TableMetadata,
) ([]IntegrityIssue, error) {
	issues := make([]IntegrityIssue, 0)

	// Find non-nullable columns
	nonNullableColumns := make([]string, 0)
	for _, col := range metadata.Columns {
		if !col.Nullable {
			nonNullableColumns = append(nonNullableColumns, col.Name)
		}
	}

	if len(nonNullableColumns) == 0 {
		// No non-nullable columns to check
		return issues, nil
	}

	// Check each non-nullable column for NULL values
	for _, colName := range nonNullableColumns {
		query := fmt.Sprintf(`
			SELECT COUNT(*) 
			FROM %s.%s 
			WHERE %s IS NULL
		`, schema, table, colName)

		rows, err := v.postgres.QueryWithTimeout(ctx, query, v.timeout)
		if err != nil {
			return nil, fmt.Errorf("failed to check null constraint for column %s: %w", colName, err)
		}

		var nullCount int64
		if rows.Next() {
			if err := rows.Scan(&nullCount); err != nil {
				rows.Close()
				return nil, fmt.Errorf("failed to scan null count for column %s: %w", colName, err)
			}
		}
		rows.Close()

		if nullCount > 0 {
			issues = append(issues, IntegrityIssue{
				IssueType:    "NULL_CONSTRAINT_VIOLATION",
				Description:  fmt.Sprintf("Non-nullable column contains NULL values"),
				ColumnName:   colName,
				AffectedRows: nullCount,
			})
			v.logger.Warn("NULL constraint violation",
				zap.String("schema", schema),
				zap.String("table", table),
				zap.String("column", colName),
				zap.Int64("nullCount", nullCount))
		}
	}

	return issues, nil
}

// checkPrimaryKeyUniqueness verifies that primary keys are unique
func (v *Verifier) checkPrimaryKeyUniqueness(
	ctx context.Context,
	schema, table string,
	primaryKeys []string,
) ([]IntegrityIssue, error) {
	issues := make([]IntegrityIssue, 0)

	if len(primaryKeys) == 0 {
		// No primary keys to check
		return issues, nil
	}

	// Build primary key column list
	pkColumns := strings.Join(primaryKeys, ", ")

	// Count duplicate primary keys
	query := fmt.Sprintf(`
		SELECT %s, COUNT(*) as count
		FROM %s.%s
		GROUP BY %s
		HAVING COUNT(*) > 1
		LIMIT 100
	`, pkColumns, schema, table, pkColumns)

	rows, err := v.postgres.QueryWithTimeout(ctx, query, v.timeout)
	if err != nil {
		return nil, fmt.Errorf("failed to check primary key uniqueness: %w", err)
	}
	defer rows.Close()

	duplicateCount := 0
	var totalAffectedRows int64 = 0

	// Calculate affected rows
	for rows.Next() {
		duplicateCount++
		// Create scannable values for all pk columns plus the count
		values := make([]interface{}, len(primaryKeys)+1)
		for i := range values {
			var val interface{}
			values[i] = &val
		}

		if err := rows.Scan(values...); err != nil {
			return nil, fmt.Errorf("failed to scan duplicate key row: %w", err)
		}

		// The last value is the count
		countVal := values[len(values)-1].(*interface{})
		count, ok := (*countVal).(int64)
		if !ok {
			// Try to convert if not int64
			countStr := fmt.Sprintf("%v", *countVal)
			parsedCount, err := strconv.ParseInt(countStr, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("failed to parse duplicate count: %w", err)
			}
			count = parsedCount
		}

		// Each duplicate group affects (count-1) rows
		// e.g., 2 duplicates = 1 affected row, 3 duplicates = 2 affected rows
		totalAffectedRows += count - 1
	}

	if duplicateCount > 0 {
		pkDescription := strings.Join(primaryKeys, ",")
		issues = append(issues, IntegrityIssue{
			IssueType:    "PRIMARY_KEY_VIOLATION",
			Description:  fmt.Sprintf("Duplicate values found for primary key (%s)", pkDescription),
			ColumnName:   pkDescription,
			AffectedRows: totalAffectedRows,
		})
		v.logger.Warn("Primary key uniqueness violation",
			zap.String("schema", schema),
			zap.String("table", table),
			zap.Strings("primaryKeys", primaryKeys),
			zap.Int("duplicateCount", duplicateCount),
			zap.Int64("affectedRows", totalAffectedRows))
	}

	return issues, nil
}

// calculateSampleSize determines appropriate sample size based on table size
func calculateSampleSize(rowCount int64) int {
	switch {
	case rowCount <= 0:
		return 0
	case rowCount < 100:
		return int(rowCount) // Sample all rows for small tables
	case rowCount < 1000:
		return 100 // 10% or 100 rows for medium tables
	case rowCount < 10000:
		return 500 // 5% for larger tables
	case rowCount < 100000:
		return 1000 // 1% for very large tables
	default:
		return 2000 // Cap at 2000 rows for huge tables
	}
}

func (v *Verifier) getTableMetadata(ctx context.Context, schema, table string) (*model.TableMetadata, error) {
	// This would typically query system tables to get metadata
	// For now, we'll implement a simplified version

	// Query column information from postgres
	query := `
		SELECT 
			column_name, 
			data_type,
			is_nullable,
			column_default,
			CASE WHEN pk.column_name IS NOT NULL THEN true ELSE false END as is_primary_key
		FROM 
			information_schema.columns c
		LEFT JOIN (
			SELECT 
				kcu.column_name
			FROM 
				information_schema.table_constraints tc
				JOIN information_schema.key_column_usage kcu
					ON tc.constraint_name = kcu.constraint_name
			WHERE 
				tc.constraint_type = 'PRIMARY KEY' AND
				tc.table_schema = $1 AND
				tc.table_name = $2
		) pk ON c.column_name = pk.column_name
		WHERE 
			c.table_schema = $1 AND
			c.table_name = $2
		ORDER BY 
			ordinal_position
	`

	rows, err := v.postgres.QueryWithTimeout(ctx, query, v.timeout, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query table metadata: %w", err)
	}
	defer rows.Close()

	columns := make([]model.Column, 0)
	primaryKeys := make([]string, 0)

	for rows.Next() {
		var (
			columnName    string
			dataType      string
			isNullable    string
			columnDefault sql.NullString
			isPrimaryKey  bool
		)

		if err := rows.Scan(&columnName, &dataType, &isNullable, &columnDefault, &isPrimaryKey); err != nil {
			return nil, fmt.Errorf("failed to scan column metadata: %w", err)
		}

		column := model.Column{
			Name:         columnName,
			DataType:     dataType,
			PgType:       dataType, // We're getting this from Postgres already
			Nullable:     isNullable == "YES",
			IsPrimaryKey: isPrimaryKey,
		}

		columns = append(columns, column)

		if isPrimaryKey {
			primaryKeys = append(primaryKeys, columnName)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating column metadata: %w", err)
	}

	metadata := &model.TableMetadata{
		Schema:      schema,
		Table:       table,
		Columns:     columns,
		PrimaryKeys: primaryKeys,
	}

	return metadata, nil
}

func (v *Verifier) getTableColumns(ctx context.Context, db connector.DatabaseConnector, schema, table string) ([]model.Column, error) {
	query := `
		SELECT 
			column_name, 
			data_type,
			CASE WHEN is_nullable = 'YES' THEN true ELSE false END as is_nullable
		FROM 
			information_schema.columns
		WHERE 
			table_schema = $1 AND
			table_name = $2
		ORDER BY 
			ordinal_position
	`

	rows, err := db.QueryWithTimeout(ctx, query, v.timeout, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	columns := make([]model.Column, 0)
	for rows.Next() {
		var (
			name     string
			dataType string
			nullable bool
		)

		if err := rows.Scan(&name, &dataType, &nullable); err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}

		columns = append(columns, model.Column{
			Name:     name,
			DataType: dataType,
			Nullable: nullable,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating columns: %w", err)
	}

	return columns, nil
}

func (v *Verifier) determineSamplingStrategy(metadata *model.TableMetadata) (string, []string) {
	if len(metadata.PrimaryKeys) > 0 {
		return "primary_key", metadata.PrimaryKeys
	}

	// Look for a unique identifier column
	for _, col := range metadata.Columns {
		if strings.Contains(strings.ToLower(col.Name), "id") ||
			strings.Contains(strings.ToLower(col.DataType), "uuid") {
			return "id_column", []string{col.Name}
		}
	}

	// Default to random sampling
	return "random", nil
}

func (v *Verifier) getSampleRows(
	ctx context.Context,
	db connector.DatabaseConnector,
	schema, table string,
	columns []model.Column,
	strategy string,
	keyColumns []string,
	sampleSize int,
) ([]map[string]interface{}, error) {
	var query string
	var args []interface{}

	// Build column list
	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = col.Name
	}
	columnList := strings.Join(columnNames, ", ")

	switch strategy {
	case "primary_key", "id_column":
		// Sample using ORDER BY key columns with LIMIT
		keyList := strings.Join(keyColumns, ", ")
		query = fmt.Sprintf(
			"SELECT %s FROM %s.%s ORDER BY %s LIMIT %d",
			columnList, schema, table, keyList, sampleSize,
		)
	case "random":
		// Random sampling
		if db == v.snowflake {
			// Snowflake random sampling
			query = fmt.Sprintf(
				"SELECT %s FROM %s.%s SAMPLE (%d ROWS)",
				columnList, schema, table, sampleSize,
			)
		} else {
			// PostgreSQL random sampling
			query = fmt.Sprintf(
				"SELECT %s FROM %s.%s ORDER BY RANDOM() LIMIT %d",
				columnList, schema, table, sampleSize,
			)
		}
	default:
		return nil, fmt.Errorf("unsupported sampling strategy: %s", strategy)
	}

	// Execute query
	rows, err := db.QueryWithTimeout(ctx, query, v.timeout, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute sample query: %w", err)
	}
	defer rows.Close()

	// Process results
	result := make([]map[string]interface{}, 0, sampleSize)
	for rows.Next() {
		// Create slice of pointers for scanning
		values := make([]interface{}, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		// Convert to map
		rowMap := make(map[string]interface{})
		for i, col := range columns {
			rowMap[col.Name] = values[i]
		}
		result = append(result, rowMap)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return result, nil
}

func (v *Verifier) formatKeyCondition(keyColumns []string, row map[string]interface{}) string {
	conditions := make([]string, 0, len(keyColumns))
	for _, key := range keyColumns {
		value := row[key]
		if value == nil {
			conditions = append(conditions, fmt.Sprintf("%s IS NULL", key))
			continue
		}

		switch v := value.(type) {
		case string:
			conditions = append(conditions, fmt.Sprintf("%s = '%s'", key, v))
		case int, int64, float64:
			conditions = append(conditions, fmt.Sprintf("%s = %v", key, v))
		default:
			// For other types, convert to string
			conditions = append(conditions, fmt.Sprintf("%s = '%v'", key, v))
		}
	}
	return strings.Join(conditions, " AND ")
}

func (v *Verifier) getMatchingRows(
	ctx context.Context,
	db connector.DatabaseConnector,
	schema, table string,
	columns []model.Column,
	keyColumns []string,
	keyConditions []string,
) (map[string]map[string]interface{}, error) {
	// Build column list
	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = col.Name
	}
	columnList := strings.Join(columnNames, ", ")

	// Build WHERE clause for the IN condition (split into batches to avoid huge queries)
	const batchSize = 100
	result := make(map[string]map[string]interface{})

	for i := 0; i < len(keyConditions); i += batchSize {
		end := i + batchSize
		if end > len(keyConditions) {
			end = len(keyConditions)
		}
		batch := keyConditions[i:end]

		whereClause := "(" + strings.Join(batch, ") OR (") + ")"
		query := fmt.Sprintf(
			"SELECT %s FROM %s.%s WHERE %s",
			columnList, schema, table, whereClause,
		)

		rows, err := db.QueryWithTimeout(ctx, query, v.timeout)
		if err != nil {
			return nil, fmt.Errorf("failed to execute matching rows query: %w", err)
		}

		// Process results
		for rows.Next() {
			// Create slice of pointers for scanning
			values := make([]interface{}, len(columns))
			scanArgs := make([]interface{}, len(columns))
			for i := range values {
				scanArgs[i] = &values[i]
			}

			if err := rows.Scan(scanArgs...); err != nil {
				rows.Close()
				return nil, fmt.Errorf("failed to scan row: %w", err)
			}

			// Convert to map
			rowMap := make(map[string]interface{})
			for i, col := range columns {
				rowMap[col.Name] = values[i]
			}

			// Create a key for this row
			rowKey := v.createRowKey(keyColumns, rowMap)
			result[rowKey] = rowMap
		}
		rows.Close()

		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("error iterating rows: %w", err)
		}
	}

	return result, nil
}

func (v *Verifier) createRowKey(keyColumns []string, row map[string]interface{}) string {
	parts := make([]string, 0, len(keyColumns))
	for _, key := range keyColumns {
		value := row[key]
		if value == nil {
			parts = append(parts, "NULL")
			continue
		}
		parts = append(parts, fmt.Sprintf("%v", value))
	}
	return strings.Join(parts, ":")
}

func (v *Verifier) compareRows(
	sourceRows []map[string]interface{},
	targetRowsMap map[string]map[string]interface{},
	columns []model.Column,
	keyColumns []string,
) []RowDiscrepancy {
	discrepancies := make([]RowDiscrepancy, 0)

	for _, sourceRow := range sourceRows {
		rowKey := v.createRowKey(keyColumns, sourceRow)
		targetRow, exists := targetRowsMap[rowKey]

		if !exists {
			// Row missing in target
			discrepancies = append(discrepancies, RowDiscrepancy{
				RowID:       rowKey,
				Discrepancy: "Row missing in target",
			})
			continue
		}

		// Compare column values
		for _, col := range columns {
			sourceVal := sourceRow[col.Name]
			targetVal := targetRow[col.Name]

			if !v.valuesEqual(sourceVal, targetVal) {
				discrepancies = append(discrepancies, RowDiscrepancy{
					RowID:       rowKey,
					ColumnName:  col.Name,
					SourceValue: sourceVal,
					TargetValue: targetVal,
					Discrepancy: "Value mismatch",
				})
			}
		}
	}

	return discrepancies
}

func (v *Verifier) valuesEqual(val1, val2 interface{}) bool {
	// Both nil
	if val1 == nil && val2 == nil {
		return true
	}

	// One nil, one not
	if (val1 == nil && val2 != nil) || (val1 != nil && val2 == nil) {
		return false
	}

	// Type-specific comparisons
	switch v1 := val1.(type) {
	case string:
		v2, ok := val2.(string)
		return ok && v1 == v2
	case int64:
		v2, ok := val2.(int64)
		if ok {
			return v1 == v2
		}
		// Try float64 conversion for database numeric types
		v2f, ok := val2.(float64)
		return ok && float64(v1) == v2f
	case float64:
		v2, ok := val2.(float64)
		if ok {
			return v1 == v2
		}
		// Try int64 conversion for database numeric types
		v2i, ok := val2.(int64)
		return ok && v1 == float64(v2i)
	case []byte:
		// Handle byte arrays, commonly used for binary data
		v2, ok := val2.([]byte)
		if !ok {
			// Try to convert string to byte array for comparison
			v2s, ok := val2.(string)
			if !ok {
				return false
			}
			return string(v1) == v2s
		}
		if len(v1) != len(v2) {
			return false
		}
		for i := range v1 {
			if v1[i] != v2[i] {
				return false
			}
		}
		return true
	case time.Time:
		v2, ok := val2.(time.Time)
		return ok && v1.Equal(v2)
	case bool:
		v2, ok := val2.(bool)
		return ok && v1 == v2
	default:
		// For other types, convert to string and compare
		return fmt.Sprintf("%v", val1) == fmt.Sprintf("%v", val2)
	}
}
