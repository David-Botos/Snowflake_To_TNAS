package transfer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/David-Botos/data-ingress/pkg/cleaner"
	"github.com/David-Botos/data-ingress/pkg/connector"
	"github.com/David-Botos/data-ingress/pkg/converter"
	"github.com/David-Botos/data-ingress/pkg/model"
)

// WorkerState represents the current state of a worker
type WorkerState string

const (
	WorkerStateIdle      WorkerState = "idle"
	WorkerStateWorking   WorkerState = "working"
	WorkerStatePaused    WorkerState = "paused"
	WorkerStateCompleted WorkerState = "completed"
	WorkerStateError     WorkerState = "error"
)

// Worker handles the execution of transfer jobs
type Worker struct {
	ID               int
	snowflake        connector.DatabaseConnector
	postgres         connector.DatabaseConnector
	dataCleaner      *cleaner.DataCleaner
	typeConverter    *converter.TypeConverter
	verifier         *Verifier
	logger           *zap.Logger
	errorHandler     *ErrorHandler
	state            WorkerState
	currentJob       *TableJob
	batchSize        int
	maxRetries       int
	stateLock        sync.RWMutex
	lastCompletedJob *TransferResult
}

// NewWorker creates a new worker
func NewWorker(
	id int,
	snowflake connector.DatabaseConnector,
	postgres connector.DatabaseConnector,
	dataCleaner *cleaner.DataCleaner,
	typeConverter *converter.TypeConverter,
	verifier *Verifier,
	errorHandler *ErrorHandler,
	logger *zap.Logger,
) *Worker {
	return &Worker{
		ID:            id,
		snowflake:     snowflake,
		postgres:      postgres,
		dataCleaner:   dataCleaner,
		typeConverter: typeConverter,
		verifier:      verifier,
		errorHandler:  errorHandler,
		logger:        logger.With(zap.Int("workerID", id)),
		state:         WorkerStateIdle,
		batchSize:     1000, // Default batch size
		maxRetries:    3,    // Default max retries
	}
}

// WithBatchSize sets the batch size for the worker
func (w *Worker) WithBatchSize(batchSize int) *Worker {
	w.batchSize = batchSize
	return w
}

// WithMaxRetries sets the maximum retry count for the worker
func (w *Worker) WithMaxRetries(maxRetries int) *Worker {
	w.maxRetries = maxRetries
	return w
}

// GetState returns the current state of the worker
func (w *Worker) GetState() WorkerState {
	w.stateLock.RLock()
	defer w.stateLock.RUnlock()
	return w.state
}

// setState updates the worker state
func (w *Worker) setState(state WorkerState) {
	w.stateLock.Lock()
	defer w.stateLock.Unlock()

	prevState := w.state
	w.state = state

	if prevState != state {
		w.logger.Info("Worker state changed",
			zap.String("from", string(prevState)),
			zap.String("to", string(state)))
	}
}

// GetCurrentJob returns the job currently being processed
func (w *Worker) GetCurrentJob() *TableJob {
	w.stateLock.RLock()
	defer w.stateLock.RUnlock()
	return w.currentJob
}

// setCurrentJob updates the current job
func (w *Worker) setCurrentJob(job *TableJob) {
	w.stateLock.Lock()
	defer w.stateLock.Unlock()
	w.currentJob = job
}

// clearCurrentJob clears the current job
func (w *Worker) clearCurrentJob() {
	w.stateLock.Lock()
	defer w.stateLock.Unlock()
	w.currentJob = nil
}

// Start begins the worker processing loop
func (w *Worker) Start(ctx context.Context, jobs <-chan TableJob, results chan<- TransferResult) {
	w.setState(WorkerStateWorking)
	w.logger.Info("Worker started")

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("Worker stopping due to context cancellation")
			w.setState(WorkerStateCompleted)
			return

		case job, ok := <-jobs:
			if !ok {
				// Channel closed, no more jobs
				w.logger.Info("Worker stopping due to closed job channel")
				w.setState(WorkerStateCompleted)
				return
			}

			w.logger.Info("Received job",
				zap.String("schema", job.Schema),
				zap.String("table", job.Table),
				zap.Int("retryCount", job.RetryCount))

			// Process the job
			result := w.ProcessJob(ctx, job)

			// Send the result
			select {
			case results <- result:
				// Result sent successfully
			case <-ctx.Done():
				w.logger.Warn("Context cancelled while sending result",
					zap.String("schema", job.Schema),
					zap.String("table", job.Table))
				w.setState(WorkerStateCompleted)
				return
			}
		}
	}
}

// ProcessJob processes a single table transfer job
func (w *Worker) ProcessJob(ctx context.Context, job TableJob) TransferResult {
	w.setCurrentJob(&job)
	w.setState(WorkerStateWorking)

	// Initialize the result
	result := NewTransferResult(job, w.ID)
	startTime := time.Now()

	w.logger.Info("Starting table transfer",
		zap.String("schema", job.Schema),
		zap.String("table", job.Table),
		zap.Int("retryCount", job.RetryCount))

	// Process the job
	success := w.transferTable(ctx, job, result)

	// Complete the result
	result.Complete(success)
	result.Duration = time.Since(startTime)

	// Log completion
	if success {
		w.logger.Info("Table transfer completed successfully",
			zap.String("schema", job.Schema),
			zap.String("table", job.Table),
			zap.Int64("rowsTransferred", result.RowsTransferred),
			zap.Duration("duration", result.Duration))
	} else {
		w.logger.Warn("Table transfer failed",
			zap.String("schema", job.Schema),
			zap.String("table", job.Table),
			zap.Int("errors", len(result.Errors)),
			zap.Duration("duration", result.Duration))
	}

	w.clearCurrentJob()
	w.setState(WorkerStateIdle)

	return *result
}

// transferTable executes the table transfer process
func (w *Worker) transferTable(ctx context.Context, job TableJob, result *TransferResult) bool {
	// Step 1: Get table metadata from Snowflake
	metadata, err := w.getTableMetadata(ctx, job.Schema, job.Table)
	if err != nil {
		errorRecord := NewErrorRecord(err, ErrorCategoryTableLevel).
			WithTable(job.Schema, job.Table)
		result.AddError(errorRecord)
		return false
	}

	// Step 2: Create target table in PostgreSQL if it doesn't exist
	err = w.createTargetTable(ctx, metadata)
	if err != nil {
		errorRecord := NewErrorRecord(err, ErrorCategoryTableLevel).
			WithTable(job.Schema, job.Table)
		result.AddError(errorRecord)
		return false
	}

	// Step 3: Transfer data in batches
	totalRows, bytesTransferred, cleaningOps, errs := w.transferData(ctx, metadata, result)

	result.RowsRead = totalRows
	result.RowsTransferred = totalRows - int64(len(errs))
	result.RowsFailed = int64(len(errs))
	result.BytesTransferred = bytesTransferred
	result.CleaningOperations = cleaningOps

	// If there were errors, add them to the result
	for _, err := range errs {
		result.AddError(err)
	}

	// Step 4: Verify the transfer if successful
	if len(errs) == 0 && w.verifier != nil {
		verified, sourceCount, targetCount, verifyErr := w.verifier.VerifyRowCount(ctx, job.Schema, job.Table)
		if verifyErr != nil {
			result.AddWarning(fmt.Sprintf("Row count verification failed with error: %v", verifyErr))
		} else if !verified {
			result.AddWarning(fmt.Sprintf("Row count verification failed: source=%d, target=%d, difference=%d",
				sourceCount, targetCount, sourceCount-targetCount))
		}
	}

	// Consider the transfer successful if we transferred some rows and didn't hit critical errors
	return result.RowsTransferred > 0 && !result.HasErrors()
}

// getTableMetadata retrieves metadata for a table
func (w *Worker) getTableMetadata(ctx context.Context, schema, table string) (*model.TableMetadata, error) {
	// Query to get column metadata from Snowflake
	query := `
		SELECT 
			COLUMN_NAME, 
			DATA_TYPE,
			CHARACTER_MAXIMUM_LENGTH,
			NUMERIC_PRECISION,
			NUMERIC_SCALE,
			IS_NULLABLE,
			COLUMN_DEFAULT,
			COMMENT
		FROM 
			INFORMATION_SCHEMA.COLUMNS
		WHERE 
			TABLE_SCHEMA = ? AND
			TABLE_NAME = ?
		ORDER BY 
			ORDINAL_POSITION
	`

	// Execute query
	rows, err := w.snowflake.QueryWithTimeout(ctx, query, time.Minute, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get table metadata: %w", err)
	}
	defer rows.Close()

	columns := make([]model.Column, 0)
	for rows.Next() {
		var (
			name             string
			dataType         string
			charMaxLength    sql.NullInt64
			numericPrecision sql.NullInt64
			numericScale     sql.NullInt64
			isNullable       string
			columnDefault    sql.NullString
			comment          sql.NullString
		)

		if err := rows.Scan(
			&name,
			&dataType,
			&charMaxLength,
			&numericPrecision,
			&numericScale,
			&isNullable,
			&columnDefault,
			&comment,
		); err != nil {
			return nil, fmt.Errorf("failed to scan column metadata: %w", err)
		}

		// Map Snowflake data type to PostgreSQL data type with fallback handling
		pgType, err := w.typeConverter.MapSnowflakeTypeToPostgres(dataType)
		if err != nil {
			// Create error record but don't fail the entire process
			errorRecord := NewErrorRecord(err, ErrorCategoryDataConversion).
				WithTable(schema, table).
				WithColumn(name, dataType)

			// Record the error
			w.errorHandler.RecordError(errorRecord)

			// Log warning but continue with fallback type
			w.logger.Warn("Using fallback type for column",
				zap.String("column", name),
				zap.String("sourceType", dataType),
				zap.String("fallbackType", "TEXT"),
				zap.Error(err))

			// Use TEXT as fallback type
			pgType = "TEXT"
		}

		column := model.Column{
			Name:     name,
			DataType: dataType,
			PgType:   pgType,
			Nullable: isNullable == "YES",
		}

		columns = append(columns, column)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating column metadata: %w", err)
	}

	// Query to get primary key information
	pkQuery := `
		SELECT 
			COLUMN_NAME
		FROM 
			INFORMATION_SCHEMA.KEY_COLUMN_USAGE
		WHERE 
			TABLE_SCHEMA = ? AND
			TABLE_NAME = ? AND
			CONSTRAINT_NAME IN (
				SELECT CONSTRAINT_NAME 
				FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS 
				WHERE CONSTRAINT_TYPE = 'PRIMARY KEY' AND
				      TABLE_SCHEMA = ? AND
				      TABLE_NAME = ?
			)
		ORDER BY 
			ORDINAL_POSITION
	`

	pkRows, err := w.snowflake.QueryWithTimeout(ctx, pkQuery, time.Minute, schema, table, schema, table)
	if err != nil {
		return nil, fmt.Errorf("failed to get primary key information: %w", err)
	}
	defer pkRows.Close()

	primaryKeys := make([]string, 0)
	for pkRows.Next() {
		var columnName string
		if err := pkRows.Scan(&columnName); err != nil {
			return nil, fmt.Errorf("failed to scan primary key column: %w", err)
		}
		primaryKeys = append(primaryKeys, columnName)

		// Mark column as primary key
		for i, col := range columns {
			if col.Name == columnName {
				col.IsPrimaryKey = true
				columns[i] = col
				break
			}
		}
	}

	if err := pkRows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating primary key columns: %w", err)
	}

	metadata := &model.TableMetadata{
		Schema:      schema,
		Table:       table,
		Columns:     columns,
		PrimaryKeys: primaryKeys,
	}

	return metadata, nil
}

// createTargetTable creates the target table in PostgreSQL if it doesn't exist
func (w *Worker) createTargetTable(ctx context.Context, metadata *model.TableMetadata) error {
	// Generate column definitions
	columnDefs, err := w.typeConverter.GenerateColumnDefinitions(metadata)
	if err != nil {
		return fmt.Errorf("failed to generate column definitions: %w", err)
	}

	// Construct CREATE TABLE query
	createTableQuery := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s.%s (%s)",
		metadata.Schema, metadata.Table, columnDefs,
	)

	// Add primary key if available
	if len(metadata.PrimaryKeys) > 0 {
		pkList := ""
		for i, pk := range metadata.PrimaryKeys {
			if i > 0 {
				pkList += ", "
			}
			pkList += pk
		}
		createTableQuery += fmt.Sprintf(", PRIMARY KEY (%s)", pkList)
	}

	// Execute query
	_, err = w.postgres.ExecWithTimeout(ctx, createTableQuery, time.Minute)
	if err != nil {
		return fmt.Errorf("failed to create target table: %w", err)
	}

	w.logger.Info("Created target table",
		zap.String("schema", metadata.Schema),
		zap.String("table", metadata.Table))

	return nil
}

// transferData transfers data from Snowflake to PostgreSQL in batches
func (w *Worker) transferData(
	ctx context.Context,
	metadata *model.TableMetadata,
	result *TransferResult,
) (int64, int64, int, []ErrorRecord) {
	// Prepare column list
	columnNames := make([]string, len(metadata.Columns))
	for i, col := range metadata.Columns {
		columnNames[i] = col.Name
	}

	// Total counters
	var totalRows int64
	var totalBytes int64
	var totalCleaningOps int
	errors := make([]ErrorRecord, 0)

	// Query to count total rows
	countQuery := fmt.Sprintf(
		"SELECT COUNT(*) FROM %s.%s",
		metadata.Schema, metadata.Table,
	)

	// Fix: Properly handle the two return values from QueryWithTimeout
	rows, err := w.snowflake.QueryWithTimeout(ctx, countQuery, time.Minute)
	if err != nil {
		errors = append(errors, NewErrorRecord(err, ErrorCategoryTableLevel).
			WithTable(metadata.Schema, metadata.Table))
		return 0, 0, 0, errors
	}

	// Make sure we close the rows when done
	defer rows.Close()

	// Scan the count result
	var rowCount int64
	if rows.Next() {
		if err := rows.Scan(&rowCount); err != nil {
			errors = append(errors, NewErrorRecord(err, ErrorCategoryTableLevel).
				WithTable(metadata.Schema, metadata.Table))
			return 0, 0, 0, errors
		}
	} else {
		errors = append(errors, NewErrorRecord(fmt.Errorf("no rows returned for count query"), ErrorCategoryTableLevel).
			WithTable(metadata.Schema, metadata.Table))
		return 0, 0, 0, errors
	}

	// Determine optimal batch size based on row count
	batchSize := w.determineBatchSize(rowCount)

	// Process data in batches
	offset := 0
	for {
		// Check if context is cancelled
		select {
		case <-ctx.Done():
			errors = append(errors, NewErrorRecord(ctx.Err(), ErrorCategorySystemLevel).
				WithTable(metadata.Schema, metadata.Table))
			return totalRows, totalBytes, totalCleaningOps, errors
		default:
			// Continue processing
		}

		// Query batch of data from Snowflake
		batchQuery := fmt.Sprintf(
			"SELECT * FROM %s.%s LIMIT %d OFFSET %d",
			metadata.Schema, metadata.Table, batchSize, offset,
		)

		sourceRows, err := w.snowflake.QueryWithTimeout(ctx, batchQuery, time.Minute*5)
		if err != nil {
			errors = append(errors, NewErrorRecord(err, ErrorCategoryChunkLevel).
				WithTable(metadata.Schema, metadata.Table))
			break
		}

		// Process the batch
		rowsProcessed, bytesProcessed, cleaningOps, batchErrors := w.processBatch(
			ctx, sourceRows, metadata, result)

		sourceRows.Close()

		// Update counters
		totalRows += rowsProcessed
		totalBytes += bytesProcessed
		totalCleaningOps += cleaningOps
		errors = append(errors, batchErrors...)

		// Log progress
		if rowsProcessed > 0 {
			w.logger.Info("Processed batch",
				zap.String("table", metadata.Table),
				zap.Int64("rowsProcessed", rowsProcessed),
				zap.Int64("totalRows", totalRows),
				zap.Int("errors", len(batchErrors)))
		}

		// If we processed fewer rows than the batch size, we're done
		if rowsProcessed < int64(batchSize) {
			break
		}

		// Move to next batch
		offset += batchSize
	}

	return totalRows, totalBytes, totalCleaningOps, errors
}

// determineBatchSize calculates an optimal batch size based on row count
func (w *Worker) determineBatchSize(rowCount int64) int {
	switch {
	case rowCount < 1000:
		return 100
	case rowCount < 10000:
		return 500
	case rowCount < 100000:
		return 1000
	case rowCount < 1000000:
		return 5000
	default:
		return 10000
	}
}

// processBatch processes a batch of rows
func (w *Worker) processBatch(
	ctx context.Context,
	sourceRows *sql.Rows,
	metadata *model.TableMetadata,
	result *TransferResult,
) (int64, int64, int, []ErrorRecord) {
	var rowsProcessed int64
	var bytesProcessed int64
	var cleaningOps int
	var successfullyTransferred int64
	errors := make([]ErrorRecord, 0)

	// Get column names and types from the result
	columnTypes, err := sourceRows.ColumnTypes()
	if err != nil {
		errorRecord := NewErrorRecord(err, ErrorCategoryChunkLevel).
			WithTable(metadata.Schema, metadata.Table)
		errors = append(errors, errorRecord)

		// Log the error and determine action
		action := w.errorHandler.HandleError(errorRecord)
		if action == ActionAbort || action == ActionSkipTable {
			return 0, 0, 0, errors
		}

		return 0, 0, 0, errors
	}

	// Log column types at debug level - can help diagnose type conversion issues
	if w.logger.Core().Enabled(zap.DebugLevel) {
		columnTypeInfo := make([]string, 0, len(columnTypes))
		for _, ct := range columnTypes {
			nullable, _ := ct.Nullable()
			columnTypeInfo = append(columnTypeInfo,
				fmt.Sprintf("%s: %s (nullable: %v)",
					ct.Name(),
					ct.DatabaseTypeName(),
					nullable,
				))
		}
		w.logger.Debug("Source column types",
			zap.String("table", metadata.Table),
			zap.Strings("columnTypes", columnTypeInfo))
	}

	// Prepare batch for insertion
	rows := make([]map[string]interface{}, 0, w.batchSize)

	// Process each row
	for sourceRows.Next() {
		// Check context cancellation
		select {
		case <-ctx.Done():
			errorRecord := NewErrorRecord(ctx.Err(), ErrorCategorySystemLevel).
				WithTable(metadata.Schema, metadata.Table)
			errors = append(errors, errorRecord)
			w.errorHandler.RecordError(errorRecord)
			return rowsProcessed, bytesProcessed, cleaningOps, errors
		default:
			// Continue processing
		}

		// Create slice of pointers for scanning
		columns, err := sourceRows.Columns()
		if err != nil {
			errorRecord := NewErrorRecord(err, ErrorCategoryRowLevel)
			errors = append(errors, errorRecord)
			w.errorHandler.RecordError(errorRecord)
			continue
		}

		values := make([]interface{}, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		// Scan the row into values
		if err := sourceRows.Scan(scanArgs...); err != nil {
			errorRecord := NewErrorRecord(err, ErrorCategoryRowLevel)
			errors = append(errors, errorRecord)
			w.errorHandler.RecordError(errorRecord)
			continue
		}

		// Create a map for the row
		row := make(map[string]interface{})
		for i, col := range columns {
			row[col] = values[i]
		}

		// Try to find a suitable row ID for error tracking
		rowID := findRowIdentifier(row, metadata)

		// Clean the row data
		cleanedRow, cleaningOperations, err := w.dataCleaner.CleanRows(
			[]map[string]interface{}{row},
			metadata,
			metadata.Schema,
			metadata.Table,
		)

		if err != nil {
			errorRecord := NewErrorRecord(err, ErrorCategoryRowLevel).
				WithTable(metadata.Schema, metadata.Table).
				WithRow(rowID)
			errors = append(errors, errorRecord)
			w.errorHandler.RecordError(errorRecord)
			continue
		}

		// If cleaning returned rows, use the first one
		if len(cleanedRow) > 0 {
			row = cleanedRow[0]
			cleaningOps += len(cleaningOperations)
		}

		// Convert values as needed
		conversionFailed := false
		for colName, val := range row {
			// Find column in metadata
			var colType string
			for _, col := range metadata.Columns {
				if strings.EqualFold(col.Name, colName) {
					colType = col.PgType
					break
				}
			}

			// Convert the value
			convertedVal, err := w.typeConverter.ConvertValueForPostgres(val, colType, colName)
			if err != nil {
				errorRecord := NewErrorRecord(err, ErrorCategoryDataConversion).
					WithTable(metadata.Schema, metadata.Table).
					WithRow(rowID).
					WithColumn(colName, val)
				errors = append(errors, errorRecord)
				w.errorHandler.RecordError(errorRecord)
				conversionFailed = true
				break
			}

			row[colName] = convertedVal
		}

		if conversionFailed {
			continue
		}

		// Add the processed row to the batch
		rows = append(rows, row)
		rowsProcessed++

		// Estimate bytes processed (rough approximation)
		for _, val := range row {
			if val != nil {
				switch v := val.(type) {
				case string:
					bytesProcessed += int64(len(v))
				case []byte:
					bytesProcessed += int64(len(v))
				default:
					bytesProcessed += 8 // Assume 8 bytes for other types
				}
			}
		}

		// If we've reached the batch size, insert the batch
		if len(rows) >= w.batchSize {
			batchInsertedRows, err := w.insertBatch(ctx, metadata, rows)
			if err != nil {
				errorRecord := NewErrorRecord(err, ErrorCategoryChunkLevel).
					WithTable(metadata.Schema, metadata.Table)
				errors = append(errors, errorRecord)
				w.errorHandler.RecordError(errorRecord)

				// Check if we should abort based on chunk error
				action := w.errorHandler.HandleError(errorRecord)
				if action == ActionSkipTable || action == ActionAbort {
					return rowsProcessed, bytesProcessed, cleaningOps, errors
				}
			} else {
				successfullyTransferred += batchInsertedRows
			}

			// Clear the batch
			rows = rows[:0]
		}
	}

	// Check for errors from the rows iterator
	if err := sourceRows.Err(); err != nil {
		errorRecord := NewErrorRecord(err, ErrorCategoryChunkLevel).
			WithTable(metadata.Schema, metadata.Table)
		errors = append(errors, errorRecord)
		w.errorHandler.RecordError(errorRecord)
	}

	// Insert any remaining rows
	if len(rows) > 0 {
		batchInsertedRows, err := w.insertBatch(ctx, metadata, rows)
		if err != nil {
			errorRecord := NewErrorRecord(err, ErrorCategoryChunkLevel).
				WithTable(metadata.Schema, metadata.Table)
			errors = append(errors, errorRecord)
			w.errorHandler.RecordError(errorRecord)
		} else {
			successfullyTransferred += batchInsertedRows
		}
	}

	return successfullyTransferred, bytesProcessed, cleaningOps, errors
}

// findRowIdentifier attempts to find a suitable identifier for the row
// for error tracking purposes
func findRowIdentifier(row map[string]interface{}, metadata *model.TableMetadata) string {
	// Try primary keys first
	if len(metadata.PrimaryKeys) > 0 {
		ids := make([]string, 0, len(metadata.PrimaryKeys))
		for _, pk := range metadata.PrimaryKeys {
			if val, ok := row[pk]; ok && val != nil {
				ids = append(ids, fmt.Sprintf("%v", val))
			}
		}

		if len(ids) > 0 {
			return strings.Join(ids, ",")
		}
	}

	// Try common ID columns
	for _, idCol := range []string{"id", "ID", "uuid", "UUID"} {
		if val, ok := row[idCol]; ok && val != nil {
			return fmt.Sprintf("%v", val)
		}
	}

	// If no suitable ID, use first non-nil value
	for colName, val := range row {
		if val != nil {
			return fmt.Sprintf("%s=%v", colName, val)
		}
	}

	// Last resort: return empty string
	return ""
}

// insertBatch inserts a batch of rows into PostgreSQL
func (w *Worker) insertBatch(
	ctx context.Context,
	metadata *model.TableMetadata,
	rows []map[string]interface{},
) (int64, error) {
	// Prepare column names and placeholders
	columns := make([]string, 0, len(metadata.Columns))
	for _, col := range metadata.Columns {
		columns = append(columns, col.Name)
	}

	// Start a transaction
	tx, err := w.postgres.DB().BeginTx(ctx, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			tx.Rollback()
		}
	}()

	// Prepare the statement with COPY
	copySQL := fmt.Sprintf(
		"COPY %s.%s (%s) FROM STDIN",
		metadata.Schema,
		metadata.Table,
		strings.Join(columns, ", "),
	)

	stmt, err := tx.Prepare(copySQL)
	if err != nil {
		return 0, fmt.Errorf("failed to prepare COPY statement: %w", err)
	}
	defer stmt.Close()

	// Execute the batch insert
	insertedRows, err := w.executeBatchInsert(ctx, tx, stmt, rows, columns)
	if err != nil {
		return 0, err
	}

	// Commit the transaction
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("failed to commit transaction: %w", err)
	}

	return insertedRows, nil
}

// GetSourceRowCount gets the total row count from Snowflake
func (w *Worker) GetSourceRowCount(ctx context.Context, schema, table string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", schema, table)

	// Handle both return values from QueryWithTimeout
	rows, err := w.snowflake.QueryWithTimeout(ctx, query, time.Minute)
	if err != nil {
		return 0, fmt.Errorf("failed to get source row count: %w", err)
	}
	defer rows.Close()

	var count int64
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return 0, fmt.Errorf("failed to scan source row count: %w", err)
		}
	} else {
		return 0, fmt.Errorf("no rows returned for count query on %s.%s", schema, table)
	}

	// Check for any errors that occurred during iteration
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("error occurred while reading count result: %w", err)
	}

	return count, nil
}

// GetTargetRowCount gets the total row count from PostgreSQL
func (w *Worker) GetTargetRowCount(ctx context.Context, schema, table string) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s.%s", schema, table)

	rows, err := w.postgres.QueryWithTimeout(ctx, query, time.Minute)
	if err != nil {
		return 0, fmt.Errorf("failed to get target row count: %w", err)
	}
	defer rows.Close()

	var count int64
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return 0, fmt.Errorf("failed to scan target row count: %w", err)
		}
	} else {
		return 0, fmt.Errorf("no rows returned for count query on %s.%s", schema, table)
	}

	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("error occurred while reading count result: %w", err)
	}

	return count, nil
}

// FetchDataInChunks fetches data from Snowflake in chunks
func (w *Worker) FetchDataInChunks(
	ctx context.Context,
	schema, table string,
	columns []string,
	chunkSize int,
	processor func(chunk [][]interface{}) error,
) (int64, error) {
	// Build the query
	columnList := strings.Join(columns, ", ")
	query := fmt.Sprintf("SELECT %s FROM %s.%s", columnList, schema, table)

	// Execute the query
	rows, err := w.snowflake.QueryWithTimeout(ctx, query, time.Hour)
	if err != nil {
		return 0, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	var totalRows int64
	chunk := make([][]interface{}, 0, chunkSize)

	// Process rows in chunks
	for rows.Next() {
		// Create slice of pointers for scanning
		values := make([]interface{}, len(columns))
		scanArgs := make([]interface{}, len(columns))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		// Scan the row
		if err := rows.Scan(scanArgs...); err != nil {
			return totalRows, fmt.Errorf("failed to scan row: %w", err)
		}

		// Add row to current chunk
		chunk = append(chunk, values)
		totalRows++

		// Process the chunk if it's full
		if len(chunk) >= chunkSize {
			if err := processor(chunk); err != nil {
				return totalRows, err
			}
			chunk = chunk[:0]
		}
	}

	// Process any remaining rows
	if len(chunk) > 0 {
		if err := processor(chunk); err != nil {
			return totalRows, err
		}
	}

	if err := rows.Err(); err != nil {
		return totalRows, fmt.Errorf("error iterating rows: %w", err)
	}

	return totalRows, nil
}

// RecordCleaningOperations records cleaning operations in tracking table
func (w *Worker) RecordCleaningOperations(
	ctx context.Context,
	operations []model.CleaningOperation,
) error {
	if len(operations) == 0 {
		return nil
	}

	return w.dataCleaner.RecordCleaningOperations(operations)
}

// executeBatchInsert performs the actual batch insert
func (w *Worker) executeBatchInsert(
	ctx context.Context,
	tx *sql.Tx,
	stmt *sql.Stmt,
	rows []map[string]interface{},
	columns []string,
) (int64, error) {
	// Implementation depends on specific database driver capabilities
	// This is a simplified implementation using individual inserts

	var insertedRows int64 = 0
	for _, row := range rows {
		// Prepare values in the correct order
		values := make([]interface{}, len(columns))
		for i, col := range columns {
			values[i] = row[col]
		}

		// Execute the insert
		_, err := stmt.Exec(values...)
		if err != nil {
			return insertedRows, fmt.Errorf("failed to insert row: %w", err)
		}

		insertedRows++
	}

	return insertedRows, nil
}
