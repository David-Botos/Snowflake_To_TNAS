package transfer

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// TableJob represents a table transfer job
type TableJob struct {
	ID           string    // Unique job identifier
	Schema       string    // Schema name
	Table        string    // Table name
	Priority     int       // Job priority (higher = more important)
	CreatedAt    time.Time // Job creation timestamp
	RetryCount   int       // Number of retries attempted
	MaxRetries   int       // Maximum allowed retries
	Dependencies []string  // IDs of jobs this depends on (if any)
}

// NewTableJob creates a new table job with defaults
func NewTableJob(schema, table string) TableJob {
	return TableJob{
		ID:         uuid.New().String(),
		Schema:     schema,
		Table:      table,
		Priority:   1, // Default priority
		CreatedAt:  time.Now(),
		RetryCount: 0,
		MaxRetries: 3, // Default max retries
	}
}

// WithPriority sets the job priority and returns the modified job
func (j TableJob) WithPriority(priority int) TableJob {
	j.Priority = priority
	return j
}

// WithMaxRetries sets the maximum retry count and returns the modified job
func (j TableJob) WithMaxRetries(maxRetries int) TableJob {
	j.MaxRetries = maxRetries
	return j
}

// WithDependencies sets job dependencies and returns the modified job
func (j TableJob) WithDependencies(deps []string) TableJob {
	j.Dependencies = deps
	return j
}

// IsRetryable checks if the job can be retried
func (j TableJob) IsRetryable() bool {
	return j.RetryCount < j.MaxRetries
}

// Retry increments the retry count and returns the modified job
func (j TableJob) Retry() TableJob {
	j.RetryCount++
	return j
}

// FullName returns the fully qualified table name
func (j TableJob) FullName() string {
	return fmt.Sprintf("%s.%s", j.Schema, j.Table)
}

// TransferResult represents the result of a table transfer
type TransferResult struct {
	JobID              string
	Schema             string
	Table              string
	Success            bool
	RowsRead           int64
	RowsTransferred    int64
	RowsFailed         int64
	BytesTransferred   int64
	CleaningOperations int
	Errors             []ErrorRecord
	Warnings           []string
	StartTime          time.Time
	EndTime            time.Time
	Duration           time.Duration
	RetryCount         int
	WorkerID           int
}

// NewTransferResult initializes a transfer result for a job
func NewTransferResult(job TableJob, workerID int) *TransferResult {
	now := time.Now()
	return &TransferResult{
		JobID:      job.ID,
		Schema:     job.Schema,
		Table:      job.Table,
		StartTime:  now,
		RetryCount: job.RetryCount,
		WorkerID:   workerID,
		Errors:     make([]ErrorRecord, 0),
		Warnings:   make([]string, 0),
	}
}

// Complete marks the transfer as complete and calculates duration
func (r *TransferResult) Complete(success bool) {
	r.EndTime = time.Now()
	r.Duration = r.EndTime.Sub(r.StartTime)
	r.Success = success
}

// AddError adds an error to the result
func (r *TransferResult) AddError(err ErrorRecord) {
	r.Errors = append(r.Errors, err)
	r.Success = false
}

// AddWarning adds a warning to the result
func (r *TransferResult) AddWarning(warning string) {
	r.Warnings = append(r.Warnings, warning)
}

// ErrorCount returns the number of errors
func (r *TransferResult) ErrorCount() int {
	return len(r.Errors)
}

// HasErrors checks if any errors occurred
func (r *TransferResult) HasErrors() bool {
	return len(r.Errors) > 0
}

// SchemaResult represents the result of a schema transfer
type SchemaResult struct {
	Schema           string
	TotalTables      int
	SuccessfulTables []string
	FailedTables     map[string]error
	SkippedTables    []string
	TotalRows        int64
	TotalBytesMoved  int64
	TotalCleaningOps int
	TotalErrors      int
	Duration         time.Duration
	StartTime        time.Time
	EndTime          time.Time
}

// NewSchemaResult initializes a new schema result
func NewSchemaResult(schema string) *SchemaResult {
	return &SchemaResult{
		Schema:           schema,
		StartTime:        time.Now(),
		SuccessfulTables: make([]string, 0),
		FailedTables:     make(map[string]error),
		SkippedTables:    make([]string, 0),
	}
}

// Complete marks the schema transfer as complete and calculates duration
func (r *SchemaResult) Complete() {
	r.EndTime = time.Now()
	r.Duration = r.EndTime.Sub(r.StartTime)
	r.TotalTables = len(r.SuccessfulTables) + len(r.FailedTables) + len(r.SkippedTables)
}

// AddTableResult incorporates a table transfer result into the schema result
func (r *SchemaResult) AddTableResult(result TransferResult) {
	if result.Success {
		r.SuccessfulTables = append(r.SuccessfulTables, result.Table)
		r.TotalRows += result.RowsTransferred
		r.TotalBytesMoved += result.BytesTransferred
		r.TotalCleaningOps += result.CleaningOperations
	} else {
		if len(result.Errors) > 0 {
			r.FailedTables[result.Table] = fmt.Errorf("%s", result.Errors[0].Message)
			r.TotalErrors += len(result.Errors)
		} else {
			r.FailedTables[result.Table] = fmt.Errorf("unknown error")
			r.TotalErrors++
		}
	}
}

// MarkSkipped marks a table as skipped
func (r *SchemaResult) MarkSkipped(tableName string) {
	r.SkippedTables = append(r.SkippedTables, tableName)
}

// SuccessRate returns the percentage of tables successfully transferred
func (r *SchemaResult) SuccessRate() float64 {
	if r.TotalTables == 0 {
		return 0
	}
	return float64(len(r.SuccessfulTables)) / float64(r.TotalTables) * 100
}

// TransferSummary represents the final transfer summary
type TransferSummary struct {
	Schemas            []string
	TotalTables        int
	SuccessfulTables   int
	FailedTables       int
	SkippedTables      int
	TotalRows          int64
	TotalBytesMoved    int64
	TotalCleaningOps   int
	ErrorCategories    map[ErrorCategory]int
	Duration           time.Duration
	StartTime          time.Time
	EndTime            time.Time
	Throughput         float64 // rows/second
	PeakMemoryUsage    int64
	PeakCPUUtilization float64
}

// NewTransferSummary initializes a new transfer summary
func NewTransferSummary() *TransferSummary {
	return &TransferSummary{
		Schemas:         make([]string, 0),
		StartTime:       time.Now(),
		ErrorCategories: make(map[ErrorCategory]int),
	}
}

// Complete marks the transfer as complete and calculates metrics
func (s *TransferSummary) Complete() {
	s.EndTime = time.Now()
	s.Duration = s.EndTime.Sub(s.StartTime)
	if s.Duration.Seconds() > 0 {
		s.Throughput = float64(s.TotalRows) / s.Duration.Seconds()
	}
}

// AddSchemaResult incorporates a schema result into the summary
func (s *TransferSummary) AddSchemaResult(result SchemaResult) {
	s.Schemas = append(s.Schemas, result.Schema)
	s.SuccessfulTables += len(result.SuccessfulTables)
	s.FailedTables += len(result.FailedTables)
	s.SkippedTables += len(result.SkippedTables)
	s.TotalTables += result.TotalTables
	s.TotalRows += result.TotalRows
	s.TotalBytesMoved += result.TotalBytesMoved
	s.TotalCleaningOps += result.TotalCleaningOps
}

// AddError adds an error to the appropriate category
func (s *TransferSummary) AddError(category ErrorCategory) {
	if _, exists := s.ErrorCategories[category]; exists {
		s.ErrorCategories[category]++
	} else {
		s.ErrorCategories[category] = 1
	}
}

// UpdateResourceUsage updates memory and CPU usage metrics
func (s *TransferSummary) UpdateResourceUsage(memoryUsage int64, cpuUtilization float64) {
	if memoryUsage > s.PeakMemoryUsage {
		s.PeakMemoryUsage = memoryUsage
	}
	if cpuUtilization > s.PeakCPUUtilization {
		s.PeakCPUUtilization = cpuUtilization
	}
}

// OverallSuccessRate returns the percentage of tables successfully transferred
func (s *TransferSummary) OverallSuccessRate() float64 {
	if s.TotalTables == 0 {
		return 0
	}
	return float64(s.SuccessfulTables) / float64(s.TotalTables) * 100
}
