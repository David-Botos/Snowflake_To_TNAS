// pkg/cleaner/cleaner.go
package cleaner

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"go.uber.org/zap"

	"github.com/David-Botos/data-ingress/pkg/model"
)

// DataCleaner handles data validation and cleaning during the ingress process
type DataCleaner struct {
	db     *sql.DB
	logger *zap.Logger
}

// NewDataCleaner creates a new DataCleaner instance and ensures tracking table exists
func NewDataCleaner(db *sql.DB, logger *zap.Logger) (*DataCleaner, error) {
	if db == nil {
		return nil, errors.New("database connection cannot be nil")
	}
	if logger == nil {
		return nil, errors.New("logger cannot be nil")
	}

	cleaner := &DataCleaner{
		db:     db,
		logger: logger,
	}

	// Ensure the cleaning table exists
	if err := cleaner.setupCleaningTable(); err != nil {
		return nil, fmt.Errorf("failed to setup cleaning table: %w", err)
	}

	return cleaner, nil
}

// setupCleaningTable ensures the cleaned_on_ingress tracking table exists
func (c *DataCleaner) setupCleaningTable() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	createTableSQL := `
		CREATE TABLE IF NOT EXISTS public.cleaned_on_ingress (
			id SERIAL PRIMARY KEY,
			schema_name TEXT NOT NULL,
			table_name TEXT NOT NULL, 
			column_name TEXT NOT NULL,
			original_value TEXT,
			new_value TEXT NOT NULL,
			row_identifier TEXT NOT NULL,
			cleaning_operation TEXT NOT NULL,
			cleaning_reason TEXT NOT NULL,
			cleaned_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
		)
	`
	_, err := c.db.ExecContext(ctx, createTableSQL)
	if err != nil {
		return fmt.Errorf("failed to create tracking table: %w", err)
	}

	c.logger.Info("Ensured cleaned_on_ingress table exists")
	return nil
}

// CleanRows cleans a batch of rows and returns cleaned data with operations performed
func (c *DataCleaner) CleanRows(
	rows []map[string]interface{},
	metadata *model.TableMetadata,
	schema, table string,
) ([]map[string]interface{}, []model.CleaningOperation, error) {
	if metadata == nil {
		return nil, nil, errors.New("table metadata cannot be nil")
	}

	cleanedRows := make([]map[string]interface{}, 0, len(rows))
	var allOperations []model.CleaningOperation

	for _, row := range rows {
		cleanedRow, operations, err := c.cleanSingleRow(row, metadata, schema, table)
		if err != nil {
			return nil, nil, err
		}

		cleanedRows = append(cleanedRows, cleanedRow)
		allOperations = append(allOperations, operations...)
	}

	// If operations were performed, record them
	if len(allOperations) > 0 {
		if err := c.RecordCleaningOperations(allOperations); err != nil {
			return cleanedRows, allOperations, fmt.Errorf("failed to record cleaning operations: %w", err)
		}
	}

	return cleanedRows, allOperations, nil
}

// cleanSingleRow validates and cleans a single data row
func (c *DataCleaner) cleanSingleRow(
	row map[string]interface{},
	metadata *model.TableMetadata,
	schema, table string,
) (map[string]interface{}, []model.CleaningOperation, error) {
	cleanedRow := make(map[string]interface{})
	var operations []model.CleaningOperation

	// First ensure ID column is present and valid (special handling)
	idValue, hasID := row["ID"]

	cleanedID, operation := ensureValidID(idValue, hasID, schema, table)
	cleanedRow["ID"] = cleanedID

	// Record cleaning operation if ID was modified
	if operation != nil {
		operations = append(operations, *operation)
	}

	// Use the ID (now guaranteed to exist) as row identifier for other columns
	rowID := cleanedID.(string)

	// Process remaining columns
	for colName, value := range row {
		// Skip ID as it's already processed
		if colName == "ID" {
			continue
		}

		// Find column metadata if available
		var colMeta *model.Column
		for _, col := range metadata.Columns {
			if col.Name == colName {
				colMeta = &col
				break
			}
		}

		// For UUID columns, ensure valid UUIDs
		if isUUIDColumn(colName) {
			cleanedValue, op := cleanUUIDValue(value, colName, rowID, schema, table)
			cleanedRow[colName] = cleanedValue
			if op != nil {
				operations = append(operations, *op)
			}
		} else if colMeta != nil && colMeta.DataType != "" {
			// Handle type standardization if metadata available
			standardizedValue, op := standardizeDataType(value, colMeta, rowID, schema, table)
			cleanedRow[colName] = standardizedValue
			if op != nil {
				operations = append(operations, *op)
			}
		} else {
			// Pass through other values (preserving nulls)
			cleanedRow[colName] = value
		}
	}

	return cleanedRow, operations, nil
}

// RecordCleaningOperations batch inserts cleaning operations into tracking table
func (c *DataCleaner) RecordCleaningOperations(operations []model.CleaningOperation) error {
	if len(operations) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Begin transaction
	tx, err := c.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer func() {
		if err != nil {
			if rbErr := tx.Rollback(); rbErr != nil {
				c.logger.Error("Failed to rollback transaction",
					zap.Error(rbErr),
					zap.Error(err))
			}
		}
	}()

	// Prepare statement
	stmt, err := tx.PrepareContext(ctx, `
		INSERT INTO public.cleaned_on_ingress
		(schema_name, table_name, column_name, original_value, new_value,
		 row_identifier, cleaning_operation, cleaning_reason)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`)
	if err != nil {
		return fmt.Errorf("failed to prepare statement: %w", err)
	}
	defer stmt.Close()

	// Execute batch insert
	for _, op := range operations {
		_, err := stmt.ExecContext(ctx,
			op.SchemaName,
			op.TableName,
			op.ColumnName,
			toNullableString(op.OriginalValue),
			op.NewValue,
			op.RowIdentifier,
			op.CleaningOperation,
			op.CleaningReason,
		)
		if err != nil {
			return fmt.Errorf("failed to insert cleaning operation: %w", err)
		}
	}

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	c.logger.Info("Recorded cleaning operations", zap.Int("count", len(operations)))
	return nil
}

// ValidateDataTypes performs data type validation without modifying values
// Returns errors for any invalid data types found
func (c *DataCleaner) ValidateDataTypes(
	rows []map[string]interface{},
	metadata *model.TableMetadata,
) ([]error, error) {
	if metadata == nil {
		return nil, errors.New("table metadata cannot be nil")
	}

	var validationErrors []error

	for i, row := range rows {
		for colName, value := range row {
			// Find column metadata
			var colMeta *model.Column
			for _, col := range metadata.Columns {
				if col.Name == colName {
					colMeta = &col
					break
				}
			}

			if colMeta == nil || colMeta.DataType == "" {
				continue // Skip validation if no metadata available
			}

			// Skip null values (they're allowed except for ID)
			if value == nil && colName != "ID" {
				continue
			}

			// Validate data type
			if err := validateDataType(value, colMeta.DataType); err != nil {
				err = fmt.Errorf("row %d, column %s: %w", i, colName, err)
				validationErrors = append(validationErrors, err)
			}
		}
	}

	return validationErrors, nil
}
