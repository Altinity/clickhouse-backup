package enhanced

import (
	"fmt"
	"strings"
)

// BatchDeleteError contains detailed error information for batch delete operations
type BatchDeleteError struct {
	TotalFiles     int
	FailedFiles    int
	FailedKeys     []FailedKey
	Errors         []error
	PartialSuccess bool
}

func (e *BatchDeleteError) Error() string {
	if e.PartialSuccess {
		return fmt.Sprintf("batch delete partially failed: %d of %d files failed to delete",
			e.FailedFiles, e.TotalFiles)
	}
	return fmt.Sprintf("batch delete failed: %d of %d files failed to delete",
		e.FailedFiles, e.TotalFiles)
}

// IsPartialFailure returns true if some files were successfully deleted
func (e *BatchDeleteError) IsPartialFailure() bool {
	return e.PartialSuccess
}

// GetFailureRate returns the ratio of failed files to total files
func (e *BatchDeleteError) GetFailureRate() float64 {
	if e.TotalFiles == 0 {
		return 0.0
	}
	return float64(e.FailedFiles) / float64(e.TotalFiles)
}

// GetFailedKeys returns a slice of all failed keys
func (e *BatchDeleteError) GetFailedKeys() []string {
	keys := make([]string, len(e.FailedKeys))
	for i, fk := range e.FailedKeys {
		keys[i] = fk.Key
	}
	return keys
}

// OptimizationConfigError represents configuration-related errors
type OptimizationConfigError struct {
	Field   string
	Value   interface{}
	Message string
}

func (e *OptimizationConfigError) Error() string {
	return fmt.Sprintf("configuration error for field %s (value: %v): %s",
		e.Field, e.Value, e.Message)
}

// CacheError represents cache-related errors
type CacheError struct {
	Operation string
	Key       string
	Cause     error
}

func (e *CacheError) Error() string {
	return fmt.Sprintf("cache %s error for key %s: %v",
		e.Operation, e.Key, e.Cause)
}

func (e *CacheError) Unwrap() error {
	return e.Cause
}

// WorkerPoolError represents errors from worker pool operations
type WorkerPoolError struct {
	WorkerID int
	Task     string
	Cause    error
}

func (e *WorkerPoolError) Error() string {
	return fmt.Sprintf("worker %d failed task %s: %v",
		e.WorkerID, e.Task, e.Cause)
}

func (e *WorkerPoolError) Unwrap() error {
	return e.Cause
}

// ValidationError represents validation errors for delete operations
type ValidationError struct {
	Field   string
	Value   interface{}
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("validation error for %s (value: %v): %s",
		e.Field, e.Value, e.Message)
}

// MetricsError represents errors in metrics collection
type MetricsError struct {
	Operation string
	Cause     error
}

func (e *MetricsError) Error() string {
	return fmt.Sprintf("metrics error during %s: %v",
		e.Operation, e.Cause)
}

func (e *MetricsError) Unwrap() error {
	return e.Cause
}

// TimeoutError represents timeout errors during batch operations
type TimeoutError struct {
	Operation string
	Duration  string
	Context   string
}

func (e *TimeoutError) Error() string {
	return fmt.Sprintf("timeout during %s after %s: %s",
		e.Operation, e.Duration, e.Context)
}

// NewBatchDeleteError creates a new BatchDeleteError from failed keys and errors
func NewBatchDeleteError(totalFiles int, failedKeys []FailedKey, errors []error) *BatchDeleteError {
	return &BatchDeleteError{
		TotalFiles:     totalFiles,
		FailedFiles:    len(failedKeys),
		FailedKeys:     failedKeys,
		Errors:         errors,
		PartialSuccess: len(failedKeys) > 0 && len(failedKeys) < totalFiles,
	}
}

// CombineErrors combines multiple errors into a single error message
func CombineErrors(errors []error) error {
	if len(errors) == 0 {
		return nil
	}
	if len(errors) == 1 {
		return errors[0]
	}

	var messages []string
	for _, err := range errors {
		if err != nil {
			messages = append(messages, err.Error())
		}
	}

	return fmt.Errorf("multiple errors occurred: %s", strings.Join(messages, "; "))
}

// IsRetriableError determines if an error should trigger a retry
func IsRetriableError(err error) bool {
	if err == nil {
		return false
	}

	// Check for specific error types that are typically retriable
	errStr := strings.ToLower(err.Error())
	retriablePatterns := []string{
		"timeout",
		"connection reset",
		"connection refused",
		"temporary failure",
		"service unavailable",
		"throttled",
		"rate limit",
		"too many requests",
	}

	for _, pattern := range retriablePatterns {
		if strings.Contains(errStr, pattern) {
			return true
		}
	}

	return false
}
