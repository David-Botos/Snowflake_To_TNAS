package transfer

import (
	"context"
	"fmt"
	"math"
	"runtime"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/David-Botos/data-ingress/pkg/cleaner"
	"github.com/David-Botos/data-ingress/pkg/connector"
	"github.com/David-Botos/data-ingress/pkg/converter"
)

// TransferManager orchestrates the data transfer process
type TransferManager struct {
	snowflake     connector.DatabaseConnector
	postgres      connector.DatabaseConnector
	typeConverter *converter.TypeConverter
	dataCleaner   *cleaner.DataCleaner
	verifier      *Verifier
	errorHandler  *ErrorHandler
	metrics       *TransferMetrics
	logger        *zap.Logger
	workers       []*Worker
	workerCount   int
	jobQueue      chan TableJob
	resultQueue   chan TransferResult
	jobMu         sync.Mutex
	pendingJobs   map[string]TableJob
	completedJobs map[string]TransferResult
	failedJobs    map[string]TransferResult
	dependencies  map[string][]string // job ID -> dependent job IDs
}

// NewTransferManager creates a new transfer manager
func NewTransferManager(
	snowflake connector.DatabaseConnector,
	postgres connector.DatabaseConnector,
	typeConverter *converter.TypeConverter,
	dataCleaner *cleaner.DataCleaner,
	logger *zap.Logger,
) *TransferManager {
	// Determine worker count based on system resources
	workerCount := calculateOptimalWorkerCount()

	// Create error handler
	errorHandler := NewErrorHandler(logger)

	// Create verifier
	verifier := NewVerifier(snowflake, postgres, logger)

	// Create metrics collector
	metrics := NewTransferMetrics(logger)

	tm := &TransferManager{
		snowflake:     snowflake,
		postgres:      postgres,
		typeConverter: typeConverter,
		dataCleaner:   dataCleaner,
		verifier:      verifier,
		errorHandler:  errorHandler,
		metrics:       metrics,
		logger:        logger,
		workerCount:   workerCount,
		jobQueue:      make(chan TableJob, workerCount*10), // Buffer size is 10x worker count
		resultQueue:   make(chan TransferResult, workerCount*10),
		pendingJobs:   make(map[string]TableJob),
		completedJobs: make(map[string]TransferResult),
		failedJobs:    make(map[string]TransferResult),
		dependencies:  make(map[string][]string),
	}

	// Create workers
	tm.createWorkers()

	return tm
}

// createWorkers initializes the worker pool
func (tm *TransferManager) createWorkers() {
	tm.workers = make([]*Worker, tm.workerCount)
	for i := 0; i < tm.workerCount; i++ {
		tm.workers[i] = NewWorker(
			i,
			tm.snowflake,
			tm.postgres,
			tm.dataCleaner,
			tm.typeConverter,
			tm.verifier,
			tm.errorHandler,
			tm.logger,
		)
	}
}

// identifyRunnableJobs finds dependent jobs that can now be run
func (tm *TransferManager) identifyRunnableJobs(completedJobID string) []TableJob {
	tm.jobMu.Lock()
	defer tm.jobMu.Unlock()

	runnableJobs := []TableJob{}

	// Look for jobs that depend on the completed job
	for jobID, depIDs := range tm.dependencies {
		// Check if this job depends on the completed job
		dependsOnCompleted := false
		for _, depID := range depIDs {
			if depID == completedJobID {
				dependsOnCompleted = true
				break
			}
		}

		if !dependsOnCompleted {
			continue
		}

		// Check if all dependencies are now satisfied
		allDepsSatisfied := true
		for _, depID := range depIDs {
			if _, completed := tm.completedJobs[depID]; !completed {
				// Dependency not yet completed
				allDepsSatisfied = false
				break
			}
		}

		if allDepsSatisfied {
			// Find the job and add it to runnable jobs
			for _, job := range tm.pendingJobs {
				if job.ID == jobID {
					runnableJobs = append(runnableJobs, job)
					break
				}
			}
		}
	}

	return runnableJobs
}

// GetTableList retrieves all tables from a schema
func (tm *TransferManager) GetTableList(ctx context.Context, schema string) ([]string, error) {
	return tm.getSchemaTableList(ctx, schema, "", "")
}

// StartWorkerPool initializes and starts worker goroutines
func (tm *TransferManager) StartWorkerPool(
	ctx context.Context,
	jobs <-chan TableJob,
	results chan<- TransferResult,
) *sync.WaitGroup {
	var wg sync.WaitGroup

	// Start each worker
	for i := 0; i < tm.workerCount; i++ {
		wg.Add(1)
		go func(worker *Worker) {
			defer wg.Done()
			worker.Start(ctx, jobs, results)
		}(tm.workers[i])
	}

	return &wg
}

// SubmitJobs sends table transfer jobs to the job channel
func (tm *TransferManager) SubmitJobs(
	ctx context.Context,
	schema string,
	tables []string,
	jobs chan<- TableJob,
) error {
	// Create jobs for tables
	tm.addJobsForTables(schema, tables)

	// Submit jobs that don't have dependencies
	tm.jobMu.Lock()
	defer tm.jobMu.Unlock()

	for _, job := range tm.pendingJobs {
		if len(job.Dependencies) == 0 {
			select {
			case jobs <- job:
				tm.logger.Debug("Submitted job",
					zap.String("schema", job.Schema),
					zap.String("table", job.Table),
					zap.String("jobID", job.ID))
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	return nil
}

// ProcessResults collects and processes worker results
func (tm *TransferManager) ProcessResults(
	results <-chan TransferResult,
	schemaResult *SchemaResult,
) {
	for result := range results {
		// Update metrics
		tm.metrics.RecordTableTransfer(result)

		// Add to schema result
		schemaResult.AddTableResult(result)

		// Check if we should abort based on errors
		if !result.Success && tm.errorHandler.ShouldAbortSchema(result.Schema) {
			tm.logger.Error("Aborting schema transfer due to error threshold",
				zap.String("schema", result.Schema))
			break
		}
	}
}

// GenerateSummaryReport creates a final transfer summary
func (tm *TransferManager) GenerateSummaryReport() *TransferSummary {
	// Get metrics summary
	return tm.metrics.GenerateTransferSummary()
}

// WithWorkerCount sets the number of worker goroutines
func (tm *TransferManager) WithWorkerCount(count int) *TransferManager {
	if count > 0 {
		tm.workerCount = count
		tm.createWorkers()
	}
	return tm
}

// TransferSchema transfers all tables in a schema
func (tm *TransferManager) TransferSchema(ctx context.Context, schema string, includePattern, excludePattern string) (*SchemaResult, error) {
	tm.logger.Info("Starting schema transfer",
		zap.String("schema", schema),
		zap.String("includePattern", includePattern),
		zap.String("excludePattern", excludePattern))

	// Start metrics tracking for this schema
	tm.metrics.StartSchemaTransfer(schema)
	schemaResult := NewSchemaResult(schema)
	startTime := time.Now()

	// Get list of tables in schema
	tables, err := tm.getSchemaTableList(ctx, schema, includePattern, excludePattern)
	if err != nil {
		return nil, fmt.Errorf("failed to get table list: %w", err)
	}

	tm.logger.Info("Found tables to transfer",
		zap.String("schema", schema),
		zap.Int("tableCount", len(tables)))

	// If no tables found, return empty result
	if len(tables) == 0 {
		schemaResult.Complete()
		schemaResult.Duration = time.Since(startTime)
		return schemaResult, nil
	}

	// Create jobs for each table
	tm.addJobsForTables(schema, tables)

	// Start result processor
	done := make(chan struct{})
	go tm.processResults(ctx, schemaResult, done)

	// Start workers
	workerCtx, cancelWorkers := context.WithCancel(ctx)
	defer cancelWorkers()

	var wg sync.WaitGroup
	for i := 0; i < tm.workerCount; i++ {
		wg.Add(1)
		go func(worker *Worker) {
			defer wg.Done()
			worker.Start(workerCtx, tm.jobQueue, tm.resultQueue)
		}(tm.workers[i])
	}

	// Submit jobs to queue (respecting dependencies)
	if err := tm.submitJobs(ctx); err != nil {
		tm.logger.Error("Error submitting jobs", zap.Error(err))
	}

	// Wait for all jobs to complete or context to be cancelled
	tm.logger.Info("Waiting for all jobs to complete")
	allJobsComplete := make(chan struct{})
	go func() {
		wg.Wait()
		close(allJobsComplete)
	}()

	select {
	case <-ctx.Done():
		tm.logger.Warn("Transfer cancelled by context")
		cancelWorkers()
	case <-allJobsComplete:
		tm.logger.Info("All workers have completed")
	}

	// Wait for result processor to finish
	close(tm.resultQueue)
	<-done

	// Complete schema metrics
	tm.metrics.EndSchemaTransfer(schema)

	// Finalize schema result
	schemaResult.Complete()
	schemaResult.Duration = time.Since(startTime)

	tm.logger.Info("Schema transfer completed",
		zap.String("schema", schema),
		zap.Int("successfulTables", len(schemaResult.SuccessfulTables)),
		zap.Int("failedTables", len(schemaResult.FailedTables)),
		zap.Int("skippedTables", len(schemaResult.SkippedTables)),
		zap.Duration("duration", schemaResult.Duration))

	return schemaResult, nil
}

// TransferAll transfers all specified schemas
func (tm *TransferManager) TransferAll(ctx context.Context, schemas []string, includePattern, excludePattern string) (*TransferSummary, error) {
	tm.logger.Info("Starting transfer of all schemas",
		zap.Strings("schemas", schemas),
		zap.String("includePattern", includePattern),
		zap.String("excludePattern", excludePattern))

	// Initialize summary
	summary := NewTransferSummary()
	startTime := time.Now()

	// Transfer each schema
	for _, schema := range schemas {
		schemaCtx, cancel := context.WithCancel(ctx)

		result, err := tm.TransferSchema(schemaCtx, schema, includePattern, excludePattern)
		if err != nil {
			tm.logger.Error("Failed to transfer schema",
				zap.String("schema", schema),
				zap.Error(err))
			cancel()
			continue
		}

		// Add schema result to summary
		summary.AddSchemaResult(*result)
		cancel()

		// Check if we should abort based on error thresholds
		if tm.errorHandler.ShouldAbortSchema(schema) {
			tm.logger.Warn("Aborting remaining schemas due to error threshold",
				zap.String("schema", schema))
			break
		}

		// Check for context cancellation
		select {
		case <-ctx.Done():
			tm.logger.Warn("Transfer cancelled by context")
			break
		default:
			// Continue to next schema
		}
	}

	// Complete the summary
	summary.Complete()
	summary.Duration = time.Since(startTime)

	// Log final summary
	tm.logger.Info("Transfer completed",
		zap.Int("successfulTables", summary.SuccessfulTables),
		zap.Int("failedTables", summary.FailedTables),
		zap.Int("skippedTables", summary.SkippedTables),
		zap.Int64("totalRows", summary.TotalRows),
		zap.Duration("duration", summary.Duration))

	return summary, nil
}

// TransferTable transfers a single table
func (tm *TransferManager) TransferTable(ctx context.Context, schema, table string) (*TransferResult, error) {
	tm.logger.Info("Starting single table transfer",
		zap.String("schema", schema),
		zap.String("table", table))

	// Create a job for this table
	job := NewTableJob(schema, table)

	// Create a dedicated worker for this job
	worker := NewWorker(
		-1, // Special ID for single-table worker
		tm.snowflake,
		tm.postgres,
		tm.dataCleaner,
		tm.typeConverter,
		tm.verifier,
		tm.errorHandler,
		tm.logger,
	)

	// Process the job
	result := worker.ProcessJob(ctx, job)

	// Record the result in metrics
	tm.metrics.RecordTableTransfer(result)

	tm.logger.Info("Single table transfer completed",
		zap.String("schema", schema),
		zap.String("table", table),
		zap.Bool("success", result.Success),
		zap.Int64("rowsTransferred", result.RowsTransferred),
		zap.Duration("duration", result.Duration))

	return &result, nil
}

// TransferAllSchemas transfers all schemas provided
func (tm *TransferManager) TransferAllSchemas(ctx context.Context, schemas []string, includePattern, excludePattern string) (*TransferSummary, error) {
	if len(schemas) == 0 {
		return nil, fmt.Errorf("no schemas provided for transfer")
	}

	return tm.TransferAll(ctx, schemas, includePattern, excludePattern)
}

// calculateOptimalWorkerCount determines the optimal number of worker goroutines
// based on available system resources and connection constraints
func calculateOptimalWorkerCount() int {
	// Get number of logical CPUs
	numCPU := runtime.NumCPU()

	// Get available memory
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	availableMemGB := float64(memStats.Sys-memStats.HeapAlloc) / 1024 / 1024 / 1024

	// Consider SSH tunnel constraints - we're going through a tunnel
	// so we need to be careful not to overwhelm it
	// A single tunnel connection can typically handle 4-8 workers efficiently
	maxWorkersPerTunnel := 6

	// Database connection pool considerations
	// Assume each worker needs 2-3 connections (source + target + occasional extra)
	connectionsPerWorker := 3
	// Assume connection pool max is around 25-50 in a normal setup
	// We should not exceed 75% of max pool size
	maxConnectionPool := 50
	maxWorkersForPool := (maxConnectionPool * 75 / 100) / connectionsPerWorker

	// Calculate based on CPU - use 75% of available CPUs
	cpuBasedWorkers := int(math.Ceil(float64(numCPU) * 0.75))

	// Calculate based on memory - assume each worker needs ~200MB
	// This is an estimate and would need to be adjusted based on real-world usage
	memoryBasedWorkers := int(availableMemGB * 5) // 5 workers per GB

	// Take the minimum of all constraints
	workerCount := min(
		cpuBasedWorkers,
		memoryBasedWorkers,
		maxWorkersPerTunnel,
		maxWorkersForPool,
	)

	// Ensure at least 2 workers and not more than 12
	if workerCount < 2 {
		workerCount = 2
	} else if workerCount > 12 {
		workerCount = 12
	}

	return workerCount
}

// min returns the minimum value among the provided integers
func min(values ...int) int {
	if len(values) == 0 {
		return 0
	}

	result := values[0]
	for _, v := range values[1:] {
		if v < result {
			result = v
		}
	}

	return result
}

// GetMetrics returns the transfer metrics
func (tm *TransferManager) GetMetrics() *TransferMetrics {
	return tm.metrics
}

// GetErrorSummary returns a summary of errors
func (tm *TransferManager) GetErrorSummary() map[ErrorCategory]int {
	return tm.errorHandler.GetErrorSummary()
}

// GenerateReport generates a comprehensive transfer report
func (tm *TransferManager) GenerateReport() string {
	return tm.metrics.GenerateMetricsReport()
}

// Helper methods

// getSchemaTableList retrieves the list of tables in a schema
func (tm *TransferManager) getSchemaTableList(ctx context.Context, schema, includePattern, excludePattern string) ([]string, error) {
	query := `
		SELECT TABLE_NAME 
		FROM INFORMATION_SCHEMA.TABLES 
		WHERE TABLE_SCHEMA = ? 
		AND TABLE_TYPE = 'BASE TABLE'
		ORDER BY TABLE_NAME
	`

	rows, err := tm.snowflake.QueryWithTimeout(ctx, query, time.Minute, schema)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	tables := make([]string, 0)
	for rows.Next() {
		var tableName string
		if err := rows.Scan(&tableName); err != nil {
			return nil, fmt.Errorf("failed to scan table name: %w", err)
		}

		// Apply include/exclude patterns
		if shouldIncludeTable(tableName, includePattern, excludePattern) {
			tables = append(tables, tableName)
		}
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating tables: %w", err)
	}

	return tables, nil
}

// shouldIncludeTable checks if a table should be included based on patterns
func shouldIncludeTable(tableName, includePattern, excludePattern string) bool {
	// If include pattern is specified, table must match it
	if includePattern != "" && !matchPattern(tableName, includePattern) {
		return false
	}

	// If exclude pattern is specified, table must not match it
	if excludePattern != "" && matchPattern(tableName, excludePattern) {
		return false
	}

	return true
}

// matchPattern checks if a string matches a pattern (using SQL LIKE syntax)
func matchPattern(s, pattern string) bool {
	// Simple implementation for common patterns
	// In production, this would use a more robust pattern matching library
	if pattern == "" {
		return true
	}
	if pattern == "*" || pattern == "%" {
		return true
	}

	// TODO: Implement proper pattern matching
	// For now, just do simple prefix/suffix matching
	if pattern[0] == '%' && pattern[len(pattern)-1] == '%' {
		substr := pattern[1 : len(pattern)-1]
		return contains(s, substr)
	}
	if pattern[0] == '%' {
		suffix := pattern[1:]
		return hasSuffix(s, suffix)
	}
	if pattern[len(pattern)-1] == '%' {
		prefix := pattern[:len(pattern)-1]
		return hasPrefix(s, prefix)
	}

	return s == pattern
}

// contains checks if string contains substring
func contains(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

// hasPrefix checks if string starts with prefix
func hasPrefix(s, prefix string) bool {
	if len(s) < len(prefix) {
		return false
	}
	return s[:len(prefix)] == prefix
}

// hasSuffix checks if string ends with suffix
func hasSuffix(s, suffix string) bool {
	if len(s) < len(suffix) {
		return false
	}
	return s[len(s)-len(suffix):] == suffix
}

// addJobsForTables creates and queues transfer jobs for tables
func (tm *TransferManager) addJobsForTables(schema string, tables []string) {
	tm.jobMu.Lock()
	defer tm.jobMu.Unlock()

	// Build dependency graph if needed
	dependencyMap := tm.buildDependencyGraph(schema, tables)

	for _, table := range tables {
		job := NewTableJob(schema, table)

		// Set job priority based on dependencies
		if deps, hasDeps := dependencyMap[table]; hasDeps {
			// Tables with dependencies get higher priority
			job = job.WithPriority(2)

			// Add dependency job IDs
			depIDs := make([]string, 0, len(deps))
			for _, depTable := range deps {
				depJob, exists := tm.pendingJobs[fmt.Sprintf("%s.%s", schema, depTable)]
				if exists {
					depIDs = append(depIDs, depJob.ID)
				}
			}

			if len(depIDs) > 0 {
				job = job.WithDependencies(depIDs)

				// Update dependencies map
				tm.dependencies[job.ID] = depIDs
			}
		}

		// Add to pending jobs
		tm.pendingJobs[fmt.Sprintf("%s.%s", schema, table)] = job
	}
}

// buildDependencyGraph analyzes table relationships and builds a dependency graph
func (tm *TransferManager) buildDependencyGraph(schema string, tables []string) map[string][]string {
	// This is a simplified implementation
	// In a real system, you would analyze foreign key relationships
	dependencies := make(map[string][]string)

	// For now, return empty dependencies
	return dependencies
}

// submitJobs submits jobs to the queue respecting dependencies
func (tm *TransferManager) submitJobs(ctx context.Context) error {
	tm.jobMu.Lock()
	defer tm.jobMu.Unlock()

	// Create a copy of pending jobs for processing
	jobs := make([]TableJob, 0, len(tm.pendingJobs))
	for _, job := range tm.pendingJobs {
		jobs = append(jobs, job)
	}

	// Sort jobs by priority (higher priority first)
	// In a real implementation, use a proper sort
	// sort.Slice(jobs, func(i, j int) bool {
	//     return jobs[i].Priority > jobs[j].Priority
	// })

	// Submit jobs without dependencies first
	for _, job := range jobs {
		if len(job.Dependencies) == 0 {
			select {
			case tm.jobQueue <- job:
				tm.logger.Debug("Submitted job to queue",
					zap.String("schema", job.Schema),
					zap.String("table", job.Table),
					zap.String("jobID", job.ID))
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	// Jobs with dependencies will be submitted by the result processor
	// when their dependencies are satisfied

	return nil
}

// processResults processes job results and queues dependent jobs
func (tm *TransferManager) processResults(ctx context.Context, schemaResult *SchemaResult, done chan<- struct{}) {
	defer close(done)

	for result := range tm.resultQueue {
		// Update result tracking
		tm.jobMu.Lock()

		// Update completed/failed jobs maps
		if result.Success {
			tm.completedJobs[result.JobID] = result
		} else {
			tm.failedJobs[result.JobID] = result
		}

		// Remove from pending jobs
		jobKey := fmt.Sprintf("%s.%s", result.Schema, result.Table)
		delete(tm.pendingJobs, jobKey)

		// Update schema result
		tm.jobMu.Unlock()

		// Update metrics
		tm.metrics.RecordTableTransfer(result)

		// Log result
		if result.Success {
			tm.logger.Info("Table transfer succeeded",
				zap.String("schema", result.Schema),
				zap.String("table", result.Table),
				zap.Int64("rowsTransferred", result.RowsTransferred),
				zap.Duration("duration", result.Duration))

			schemaResult.AddTableResult(result)

		} else {
			tm.logger.Warn("Table transfer failed",
				zap.String("schema", result.Schema),
				zap.String("table", result.Table),
				zap.Int("errors", len(result.Errors)),
				zap.Duration("duration", result.Duration))

			schemaResult.AddTableResult(result)

			// Check if the error handler wants to abort the schema
			if tm.errorHandler.ShouldAbortSchema(result.Schema) {
				tm.logger.Error("Aborting schema transfer due to error threshold",
					zap.String("schema", result.Schema))
				return
			}
		}

		// Check for dependent jobs that can now be submitted
		dependentJobs := tm.identifyRunnableJobs(result.JobID)
		if len(dependentJobs) > 0 {
			tm.logger.Debug("Submitting dependent jobs",
				zap.Int("count", len(dependentJobs)))

			for _, job := range dependentJobs {
				select {
				case tm.jobQueue <- job:
					tm.logger.Debug("Submitted dependent job",
						zap.String("schema", job.Schema),
						zap.String("table", job.Table),
						zap.String("jobID", job.ID))
				case <-ctx.Done():
					tm.logger.Warn("Context cancelled while submitting dependent jobs")
					return
				}
			}
		}
	}
}
