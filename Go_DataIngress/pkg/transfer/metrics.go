package transfer

import (
	"encoding/json"
	"fmt"
	"runtime"
	"sync"
	"time"

	"go.uber.org/zap"
)

// SchemaMetrics tracks metrics for a specific schema
type SchemaMetrics struct {
	SchemaName         string
	StartTime          time.Time
	EndTime            time.Time
	SuccessfulTables   []string
	FailedTables       map[string]string // table name -> error message
	SkippedTables      []string
	RowsRead           int64
	RowsWritten        int64
	BytesMoved         int64
	CleaningOperations int
}

// NewSchemaMetrics creates a new schema metrics tracker
func NewSchemaMetrics(schema string) *SchemaMetrics {
	return &SchemaMetrics{
		SchemaName:       schema,
		StartTime:        time.Now(),
		SuccessfulTables: make([]string, 0),
		FailedTables:     make(map[string]string),
		SkippedTables:    make([]string, 0),
	}
}

// Duration returns the total duration of the schema transfer
func (sm *SchemaMetrics) Duration() time.Duration {
	if sm.EndTime.IsZero() {
		return time.Since(sm.StartTime)
	}
	return sm.EndTime.Sub(sm.StartTime)
}

// TotalTables returns the total number of tables processed
func (sm *SchemaMetrics) TotalTables() int {
	return len(sm.SuccessfulTables) + len(sm.FailedTables) + len(sm.SkippedTables)
}

// TransferMetrics tracks metrics for the transfer process
type TransferMetrics struct {
	mu                 sync.Mutex
	logger             *zap.Logger
	StartTime          time.Time
	EndTime            time.Time
	SchemaMetrics      map[string]*SchemaMetrics
	SuccessfulTables   int
	FailedTables       int
	SkippedTables      int
	TotalRowsRead      int64
	TotalRowsWritten   int64
	TotalBytesMoved    int64
	TotalCleaningOps   int
	PeakMemoryUsage    int64
	PeakCPUUtilization float64
	ErrorCounts        map[ErrorCategory]int
	WorkerUtilization  map[int]time.Duration
	ThroughputSamples  []ThroughputSample
	sampleInterval     time.Duration
	lastSampleTime     time.Time
}

// ThroughputSample represents a point-in-time throughput measurement
type ThroughputSample struct {
	Timestamp      time.Time
	RowsPerSecond  float64
	BytesPerSecond int64
	ActiveWorkers  int
	MemoryUsageMB  float64
}

// NewTransferMetrics creates a new TransferMetrics instance
func NewTransferMetrics(logger *zap.Logger) *TransferMetrics {
	return &TransferMetrics{
		StartTime:         time.Now(),
		SchemaMetrics:     make(map[string]*SchemaMetrics),
		ErrorCounts:       make(map[ErrorCategory]int),
		WorkerUtilization: make(map[int]time.Duration),
		ThroughputSamples: make([]ThroughputSample, 0),
		sampleInterval:    time.Second * 30, // Sample throughput every 30 seconds
		lastSampleTime:    time.Now(),
		logger:            logger,
	}
}

// StartSchemaTransfer begins tracking metrics for a schema
func (tm *TransferMetrics) StartSchemaTransfer(schema string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	sm := NewSchemaMetrics(schema)
	tm.SchemaMetrics[schema] = sm

	if tm.logger != nil {
		tm.logger.Info("Started schema transfer",
			zap.String("schema", schema),
			zap.Time("startTime", sm.StartTime))
	}
}

// EndSchemaTransfer completes tracking metrics for a schema
func (tm *TransferMetrics) EndSchemaTransfer(schema string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if sm, ok := tm.SchemaMetrics[schema]; ok {
		sm.EndTime = time.Now()

		if tm.logger != nil {
			tm.logger.Info("Completed schema transfer",
				zap.String("schema", schema),
				zap.Duration("duration", sm.Duration()),
				zap.Int("successfulTables", len(sm.SuccessfulTables)),
				zap.Int("failedTables", len(sm.FailedTables)),
				zap.Int("skippedTables", len(sm.SkippedTables)),
				zap.Int64("rowsTransferred", sm.RowsWritten))
		}
	}
}

// RecordTableTransfer records metrics for a completed table transfer
func (tm *TransferMetrics) RecordTableTransfer(result TransferResult) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Update global counters
	tm.TotalRowsRead += result.RowsRead
	tm.TotalRowsWritten += result.RowsTransferred
	tm.TotalBytesMoved += result.BytesTransferred
	tm.TotalCleaningOps += result.CleaningOperations

	if result.Success {
		tm.SuccessfulTables++
	} else {
		tm.FailedTables++
		// Record errors
		for _, err := range result.Errors {
			tm.RecordError(err.Category)
		}
	}

	// Update schema-specific metrics
	sm, exists := tm.SchemaMetrics[result.Schema]
	if exists {
		if result.Success {
			sm.SuccessfulTables = append(sm.SuccessfulTables, result.Table)
		} else if len(result.Errors) > 0 {
			errorMsg := result.Errors[0].Message
			sm.FailedTables[result.Table] = errorMsg
		} else {
			sm.FailedTables[result.Table] = "unknown error"
		}

		sm.RowsRead += result.RowsRead
		sm.RowsWritten += result.RowsTransferred
		sm.BytesMoved += result.BytesTransferred
		sm.CleaningOperations += result.CleaningOperations
	}

	// Record worker utilization
	tm.WorkerUtilization[result.WorkerID] += result.Duration

	// Check if we should take a throughput sample
	now := time.Now()
	if now.Sub(tm.lastSampleTime) >= tm.sampleInterval {
		tm.takeThroughputSample()
		tm.lastSampleTime = now
	}

	// Log result if logger is available
	if tm.logger != nil {
		tm.logger.Info("Table transfer completed",
			zap.String("schema", result.Schema),
			zap.String("table", result.Table),
			zap.Bool("success", result.Success),
			zap.Int64("rowsRead", result.RowsRead),
			zap.Int64("rowsTransferred", result.RowsTransferred),
			zap.Duration("duration", result.Duration),
			zap.Int("worker", result.WorkerID))
	}
}

// RecordSkippedTable marks a table as skipped
func (tm *TransferMetrics) RecordSkippedTable(schema, table string, reason string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tm.SkippedTables++

	// Update schema metrics if available
	if sm, exists := tm.SchemaMetrics[schema]; exists {
		sm.SkippedTables = append(sm.SkippedTables, table)
	}

	if tm.logger != nil {
		tm.logger.Info("Skipped table transfer",
			zap.String("schema", schema),
			zap.String("table", table),
			zap.String("reason", reason))
	}
}

// RecordError increments the count for a specific error category
func (tm *TransferMetrics) RecordError(category ErrorCategory) {
	// No lock needed as this is always called from within a locked method
	tm.ErrorCounts[category]++
}

// RecordWorkerUtilization tracks worker activity time
func (tm *TransferMetrics) RecordWorkerUtilization(workerID int, duration time.Duration) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	current, exists := tm.WorkerUtilization[workerID]
	if exists {
		tm.WorkerUtilization[workerID] = current + duration
	} else {
		tm.WorkerUtilization[workerID] = duration
	}
}

// RecordResourceUtilization tracks system resource usage
func (tm *TransferMetrics) RecordResourceUtilization(memoryUsage int64, cpuUtilization float64) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	if memoryUsage > tm.PeakMemoryUsage {
		tm.PeakMemoryUsage = memoryUsage
	}

	if cpuUtilization > tm.PeakCPUUtilization {
		tm.PeakCPUUtilization = cpuUtilization
	}
}

// GetCurrentResourceUsage retrieves the current resource usage
func (tm *TransferMetrics) GetCurrentResourceUsage() (int64, float64) {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	memoryUsage := int64(memStats.Alloc)

	// Note: Getting accurate CPU utilization in Go requires platform-specific code
	// or external libraries. This is a simplified placeholder.
	cpuUtilization := 0.0

	return memoryUsage, cpuUtilization
}

// takeThroughputSample records a throughput sample point
func (tm *TransferMetrics) takeThroughputSample() {
	elapsedTime := time.Since(tm.StartTime).Seconds()
	if elapsedTime <= 0 {
		return
	}

	// Calculate current throughput
	rowsPerSecond := float64(tm.TotalRowsWritten) / elapsedTime
	bytesPerSecond := int64(float64(tm.TotalBytesMoved) / elapsedTime)

	// Get current resource usage
	memoryUsage, _ := tm.GetCurrentResourceUsage()
	memoryUsageMB := float64(memoryUsage) / (1024 * 1024)

	// Count active workers (simplified - could be more sophisticated)
	activeWorkers := len(tm.WorkerUtilization)

	sample := ThroughputSample{
		Timestamp:      time.Now(),
		RowsPerSecond:  rowsPerSecond,
		BytesPerSecond: bytesPerSecond,
		ActiveWorkers:  activeWorkers,
		MemoryUsageMB:  memoryUsageMB,
	}

	tm.ThroughputSamples = append(tm.ThroughputSamples, sample)
}

// Complete marks the transfer process as complete
func (tm *TransferMetrics) Complete() {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	tm.EndTime = time.Now()

	// Take a final throughput sample
	tm.takeThroughputSample()

	// Log completion if logger is available
	if tm.logger != nil {
		duration := tm.EndTime.Sub(tm.StartTime)
		tm.logger.Info("Transfer process completed",
			zap.Duration("totalDuration", duration),
			zap.Int("successfulTables", tm.SuccessfulTables),
			zap.Int("failedTables", tm.FailedTables),
			zap.Int("skippedTables", tm.SkippedTables),
			zap.Int64("totalRowsWritten", tm.TotalRowsWritten),
			zap.Float64("throughput", tm.CalculateThroughput()))
	}
}

// CalculateThroughput calculates the rows/second throughput
func (tm *TransferMetrics) CalculateThroughput() float64 {
	duration := tm.Duration().Seconds()
	if duration <= 0 {
		return 0
	}
	return float64(tm.TotalRowsWritten) / duration
}

// Duration returns the total duration of the transfer process
func (tm *TransferMetrics) Duration() time.Duration {
	if tm.EndTime.IsZero() {
		return time.Since(tm.StartTime)
	}
	return tm.EndTime.Sub(tm.StartTime)
}

// GetWorkerEfficiency calculates worker efficiency metrics
func (tm *TransferMetrics) GetWorkerEfficiency() map[int]float64 {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	efficiency := make(map[int]float64)
	totalDuration := tm.Duration()

	if totalDuration <= 0 {
		return efficiency
	}

	for workerID, duration := range tm.WorkerUtilization {
		efficiency[workerID] = float64(duration) / float64(totalDuration)
	}

	return efficiency
}

// GetErrorDistribution returns error distribution by category
func (tm *TransferMetrics) GetErrorDistribution() map[ErrorCategory]float64 {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	distribution := make(map[ErrorCategory]float64)
	totalErrors := 0

	// Calculate total errors
	for _, count := range tm.ErrorCounts {
		totalErrors += count
	}

	if totalErrors == 0 {
		return distribution
	}

	// Calculate percentage for each category
	for category, count := range tm.ErrorCounts {
		distribution[category] = float64(count) / float64(totalErrors) * 100
	}

	return distribution
}

// GetSchemaEfficiency calculates efficiency metrics for each schema
func (tm *TransferMetrics) GetSchemaEfficiency() map[string]float64 {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	efficiency := make(map[string]float64)

	for schema, metrics := range tm.SchemaMetrics {
		totalTables := metrics.TotalTables()
		if totalTables == 0 {
			efficiency[schema] = 0
			continue
		}

		successRate := float64(len(metrics.SuccessfulTables)) / float64(totalTables) * 100
		efficiency[schema] = successRate
	}

	return efficiency
}

// GenerateTransferSummary creates a TransferSummary from metrics
func (tm *TransferMetrics) GenerateTransferSummary() *TransferSummary {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	schemas := make([]string, 0, len(tm.SchemaMetrics))
	for schema := range tm.SchemaMetrics {
		schemas = append(schemas, schema)
	}

	var endTime = tm.EndTime

	if tm.EndTime.IsZero() {
		endTime = time.Now()
	}

	summary := &TransferSummary{
		Schemas:            schemas,
		TotalTables:        tm.SuccessfulTables + tm.FailedTables + tm.SkippedTables,
		SuccessfulTables:   tm.SuccessfulTables,
		FailedTables:       tm.FailedTables,
		SkippedTables:      tm.SkippedTables,
		TotalRows:          tm.TotalRowsWritten,
		TotalBytesMoved:    tm.TotalBytesMoved,
		TotalCleaningOps:   tm.TotalCleaningOps,
		ErrorCategories:    tm.ErrorCounts,
		Duration:           tm.Duration(),
		StartTime:          tm.StartTime,
		EndTime:            endTime,
		Throughput:         tm.CalculateThroughput(),
		PeakMemoryUsage:    tm.PeakMemoryUsage,
		PeakCPUUtilization: tm.PeakCPUUtilization,
	}

	return summary
}

// formatBytes converts bytes to a human-readable string
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// formatDuration formats a duration to a human-readable string
func formatDuration(d time.Duration) string {
	hours := int(d.Hours())
	minutes := int(d.Minutes()) % 60
	seconds := int(d.Seconds()) % 60

	if hours > 0 {
		return fmt.Sprintf("%dh %dm %ds", hours, minutes, seconds)
	} else if minutes > 0 {
		return fmt.Sprintf("%dm %ds", minutes, seconds)
	}
	return fmt.Sprintf("%.2fs", d.Seconds())
}

// GenerateMetricsReport creates a detailed metrics report
func (tm *TransferMetrics) GenerateMetricsReport() string {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	report := fmt.Sprintf(`
Transfer Metrics Report
======================
Duration:                %s
Start Time:              %s
End Time:                %s

Tables Summary
-------------
Total Tables:            %d
Successful Tables:       %d (%.1f%%)
Failed Tables:           %d (%.1f%%)
Skipped Tables:          %d (%.1f%%)

Data Summary
-----------
Total Rows Read:         %d
Total Rows Written:      %d
Total Data Transferred:  %s
Total Cleaning Ops:      %d
Average Throughput:      %.2f rows/sec

Resource Usage
-------------
Peak Memory Usage:       %s
Peak CPU Utilization:    %.1f%%
`,
		formatDuration(tm.Duration()),
		tm.StartTime.Format(time.RFC3339),
		tm.EndTime.Format(time.RFC3339),

		tm.SuccessfulTables+tm.FailedTables+tm.SkippedTables,
		tm.SuccessfulTables, tm.getPercentage(float64(tm.SuccessfulTables), float64(tm.SuccessfulTables+tm.FailedTables+tm.SkippedTables)),
		tm.FailedTables, tm.getPercentage(float64(tm.FailedTables), float64(tm.SuccessfulTables+tm.FailedTables+tm.SkippedTables)),
		tm.SkippedTables, tm.getPercentage(float64(tm.SkippedTables), float64(tm.SuccessfulTables+tm.FailedTables+tm.SkippedTables)),

		tm.TotalRowsRead,
		tm.TotalRowsWritten,
		formatBytes(tm.TotalBytesMoved),
		tm.TotalCleaningOps,
		tm.CalculateThroughput(),

		formatBytes(tm.PeakMemoryUsage),
		tm.PeakCPUUtilization*100,
	)

	// Add schema details
	report += "\nSchema Details\n--------------\n"
	for schemaName, metrics := range tm.SchemaMetrics {
		successRate := tm.getPercentage(float64(len(metrics.SuccessfulTables)), float64(metrics.TotalTables()))
		report += fmt.Sprintf("- %s: %d tables, %.1f%% success, %s, %d rows\n",
			schemaName,
			metrics.TotalTables(),
			successRate,
			formatDuration(metrics.Duration()),
			metrics.RowsWritten)
	}

	// Add error distribution
	if len(tm.ErrorCounts) > 0 {
		report += "\nError Distribution\n-----------------\n"
		totalErrors := 0
		for _, count := range tm.ErrorCounts {
			totalErrors += count
		}

		for category, count := range tm.ErrorCounts {
			percentage := tm.getPercentage(float64(count), float64(totalErrors))
			report += fmt.Sprintf("- %s: %d (%.1f%%)\n", category.String(), count, percentage)
		}
	}

	// Add worker efficiency
	report += "\nWorker Efficiency\n----------------\n"
	efficiency := tm.GetWorkerEfficiency()
	for workerID, eff := range efficiency {
		report += fmt.Sprintf("- Worker %d: %.1f%% active time\n", workerID, eff*100)
	}

	return report
}

// getPercentage safely calculates a percentage, avoiding division by zero
func (tm *TransferMetrics) getPercentage(value, total float64) float64 {
	if total == 0 {
		return 0
	}
	return (value / total) * 100
}

// ToJSON serializes metrics to JSON
func (tm *TransferMetrics) ToJSON() ([]byte, error) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	return json.Marshal(struct {
		Duration          string                    `json:"duration"`
		SuccessfulTables  int                       `json:"successfulTables"`
		FailedTables      int                       `json:"failedTables"`
		SkippedTables     int                       `json:"skippedTables"`
		TotalRowsWritten  int64                     `json:"totalRowsWritten"`
		TotalBytesMoved   int64                     `json:"totalBytesMoved"`
		Throughput        float64                   `json:"throughput"`
		SchemaEfficiency  map[string]float64        `json:"schemaEfficiency"`
		ErrorDistribution map[ErrorCategory]float64 `json:"errorDistribution"`
	}{
		Duration:          formatDuration(tm.Duration()),
		SuccessfulTables:  tm.SuccessfulTables,
		FailedTables:      tm.FailedTables,
		SkippedTables:     tm.SkippedTables,
		TotalRowsWritten:  tm.TotalRowsWritten,
		TotalBytesMoved:   tm.TotalBytesMoved,
		Throughput:        tm.CalculateThroughput(),
		SchemaEfficiency:  tm.GetSchemaEfficiency(),
		ErrorDistribution: tm.GetErrorDistribution(),
	})
}
