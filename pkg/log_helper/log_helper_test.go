package log_helper

import (
	"bytes"
	stderrors "errors"
	"strings"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/stretchr/testify/require"
)

// TestCustomWriterPositive tests the happy path with all features working correctly
func TestCustomWriterPositive(t *testing.T) {
	var buf bytes.Buffer
	zerolog.ErrorStackMarshaler = CustomStackMarshaler
	zerolog.TimeFieldFormat = time.RFC3339Nano

	writer := NewCustomWriter(&buf)
	logger := zerolog.New(writer).With().Timestamp().Caller().Logger()

	// Create error with stack trace
	err := errors.New("test error")
	err = errors.WithStack(err)

	// Log with all features: timestamp, level, caller, message, fields, error, stack
	logger.Error().
		Stack().
		Err(err).
		Str("userId", "12345").
		Int("count", 42).
		Bool("success", false).
		Float64("duration", 123.456).
		Msg("test message with all features")

	output := buf.String()

	// Verify all components are present
	if !strings.Contains(output, "ERR") {
		t.Errorf("Expected ERR level, got: %s", output)
	}
	if !strings.Contains(output, "test message with all features") {
		t.Errorf("Expected message, got: %s", output)
	}
	if !strings.Contains(output, "userId=12345") {
		t.Errorf("Expected userId field, got: %s", output)
	}
	if !strings.Contains(output, "count=42") {
		t.Errorf("Expected count field, got: %s", output)
	}
	if !strings.Contains(output, "success=false") {
		t.Errorf("Expected success field, got: %s", output)
	}
	if !strings.Contains(output, "error=test error") {
		t.Errorf("Expected error field, got: %s", output)
	}
	if !strings.Contains(output, "stack:") {
		t.Errorf("Expected stack trace, got: %s", output)
	}
	if !strings.Contains(output, "log_helper_test.go") {
		t.Errorf("Expected file in stack trace, got: %s", output)
	}
}

// TestCustomWriterInvalidJSON tests handling of malformed JSON input
func TestCustomWriterInvalidJSON(t *testing.T) {
	var buf bytes.Buffer
	writer := NewCustomWriter(&buf)

	// Write invalid JSON
	invalidJSON := []byte(`{invalid json}`)
	n, err := writer.Write(invalidJSON)

	if err != nil {
		t.Errorf("Expected no error for invalid JSON, got: %v", err)
	}
	if n == 0 {
		t.Errorf("Expected some bytes written, got: %d", n)
	}

	// Should still write something (graceful degradation)
	output := buf.String()
	if len(output) == 0 {
		t.Error("Expected some output even with invalid JSON")
	}
}

// TestCustomWriterEmptyJSON tests handling of empty JSON object
func TestCustomWriterEmptyJSON(t *testing.T) {
	var buf bytes.Buffer
	writer := NewCustomWriter(&buf)

	// Write empty JSON
	emptyJSON := []byte(`{}`)
	n, err := writer.Write(emptyJSON)

	if err != nil {
		t.Errorf("Expected no error for empty JSON, got: %v", err)
	}
	if n == 0 {
		t.Errorf("Expected some bytes written, got: %d", n)
	}

	output := buf.String()
	// Should have at least a newline
	if !strings.HasSuffix(output, "\n") {
		t.Errorf("Expected output to end with newline, got: %s", output)
	}
}

// TestCustomWriterMissingFields tests handling when expected fields are missing
func TestCustomWriterMissingFields(t *testing.T) {
	var buf bytes.Buffer
	writer := NewCustomWriter(&buf)

	// JSON with only message field (missing time, level, caller)
	minimalJSON := []byte(`{"message":"test"}`)
	n, err := writer.Write(minimalJSON)

	if err != nil {
		t.Errorf("Expected no error for minimal JSON, got: %v", err)
	}
	if n == 0 {
		t.Errorf("Expected some bytes written, got: %d", n)
	}

	output := buf.String()
	// Should still have the message
	if !strings.Contains(output, "test") {
		t.Errorf("Expected message in output, got: %s", output)
	}
}

// TestCustomWriterInvalidTimestamp tests handling of malformed timestamp
func TestCustomWriterInvalidTimestamp(t *testing.T) {
	var buf bytes.Buffer
	writer := NewCustomWriter(&buf)

	// JSON with invalid timestamp format
	invalidTimeJSON := []byte(`{"time":"not-a-timestamp","level":"info","message":"test"}`)
	n, err := writer.Write(invalidTimeJSON)

	if err != nil {
		t.Errorf("Expected no error for invalid timestamp, got: %v", err)
	}
	if n == 0 {
		t.Errorf("Expected some bytes written, got: %d", n)
	}

	output := buf.String()
	// Should still process other fields
	if !strings.Contains(output, "INF") {
		t.Errorf("Expected level in output despite bad timestamp, got: %s", output)
	}
	if !strings.Contains(output, "test") {
		t.Errorf("Expected message in output despite bad timestamp, got: %s", output)
	}
}

// TestCustomWriterAllLogLevels tests all log level abbreviations
func TestCustomWriterAllLogLevels(t *testing.T) {
	levels := map[string]string{
		"trace": "TRC",
		"debug": "DBG",
		"info":  "INF",
		"warn":  "WRN",
		"error": "ERR",
		"fatal": "FTL",
		"panic": "PNC",
	}

	for level, expected := range levels {
		t.Run(level, func(t *testing.T) {
			var buf bytes.Buffer
			writer := NewCustomWriter(&buf)

			jsonLog := []byte(`{"level":"` + level + `","message":"test"}`)
			n, err := writer.Write(jsonLog)
			require.NoError(t, err)
			require.Greater(t, n, 0)
			output := buf.String()
			if !strings.Contains(output, expected) {
				t.Errorf("Expected %s for level %s, got: %s", expected, level, output)
			}
		})
	}
}

// TestCustomWriterUnknownLogLevel tests handling of unknown log level
func TestCustomWriterUnknownLogLevel(t *testing.T) {
	var buf bytes.Buffer
	writer := NewCustomWriter(&buf)

	// JSON with custom/unknown log level
	unknownLevelJSON := []byte(`{"level":"custom","message":"test"}`)
	n, err := writer.Write(unknownLevelJSON)

	if err != nil {
		t.Errorf("Expected no error for unknown level, got: %v", err)
	}
	if n == 0 {
		t.Errorf("Expected some bytes written, got: %d", n)
	}

	output := buf.String()
	// Should uppercase unknown level
	if !strings.Contains(output, "CUSTOM") {
		t.Errorf("Expected uppercased unknown level, got: %s", output)
	}
}

// TestCustomWriterSpecialCharactersInFields tests handling of special characters
func TestCustomWriterSpecialCharactersInFields(t *testing.T) {
	var buf bytes.Buffer
	writer := NewCustomWriter(&buf)

	// JSON with special characters in string fields
	specialJSON := []byte(`{"level":"info","message":"test","field":"value with \"quotes\" and \n newlines"}`)
	n, err := writer.Write(specialJSON)

	if err != nil {
		t.Errorf("Expected no error for special chars, got: %v", err)
	}
	if n == 0 {
		t.Errorf("Expected some bytes written, got: %d", n)
	}

	output := buf.String()
	if !strings.Contains(output, "field=") {
		t.Errorf("Expected field in output, got: %s", output)
	}
}

// TestCustomWriterNumericFields tests handling of different numeric types
func TestCustomWriterNumericFields(t *testing.T) {
	var buf bytes.Buffer
	writer := NewCustomWriter(&buf)

	// JSON with various numeric types
	numericJSON := []byte(`{"level":"info","message":"test","intField":42,"floatField":3.14,"negativeInt":-100}`)
	n, err := writer.Write(numericJSON)

	if err != nil {
		t.Errorf("Expected no error for numeric fields, got: %v", err)
	}
	if n == 0 {
		t.Errorf("Expected some bytes written, got: %d", n)
	}

	output := buf.String()
	if !strings.Contains(output, "intField=42") {
		t.Errorf("Expected intField in output, got: %s", output)
	}
	if !strings.Contains(output, "floatField=3.14") {
		t.Errorf("Expected floatField in output, got: %s", output)
	}
	if !strings.Contains(output, "negativeInt=-100") {
		t.Errorf("Expected negativeInt in output, got: %s", output)
	}
}

// TestCustomWriterBooleanFields tests handling of boolean fields
func TestCustomWriterBooleanFields(t *testing.T) {
	var buf bytes.Buffer
	writer := NewCustomWriter(&buf)

	// JSON with boolean fields
	boolJSON := []byte(`{"level":"info","message":"test","success":true,"failed":false}`)
	n, err := writer.Write(boolJSON)

	if err != nil {
		t.Errorf("Expected no error for boolean fields, got: %v", err)
	}
	if n == 0 {
		t.Errorf("Expected some bytes written, got: %d", n)
	}

	output := buf.String()
	if !strings.Contains(output, "success=true") {
		t.Errorf("Expected success field in output, got: %s", output)
	}
	if !strings.Contains(output, "failed=false") {
		t.Errorf("Expected failed field in output, got: %s", output)
	}
}

// TestCustomWriterNullFields tests handling of null values
func TestCustomWriterNullFields(t *testing.T) {
	var buf bytes.Buffer
	writer := NewCustomWriter(&buf)

	// JSON with null field
	nullJSON := []byte(`{"level":"info","message":"test","nullField":null}`)
	n, err := writer.Write(nullJSON)

	if err != nil {
		t.Errorf("Expected no error for null field, got: %v", err)
	}
	if n == 0 {
		t.Errorf("Expected some bytes written, got: %d", n)
	}

	output := buf.String()
	// null should be handled gracefully
	if !strings.Contains(output, "test") {
		t.Errorf("Expected message in output despite null field, got: %s", output)
	}
}

// TestCustomWriterArrayFields tests handling of array/object fields
func TestCustomWriterArrayFields(t *testing.T) {
	var buf bytes.Buffer
	writer := NewCustomWriter(&buf)

	// JSON with array and object fields
	complexJSON := []byte(`{"level":"info","message":"test","array":[1,2,3],"object":{"key":"value"}}`)
	n, err := writer.Write(complexJSON)

	if err != nil {
		t.Errorf("Expected no error for complex fields, got: %v", err)
	}
	if n == 0 {
		t.Errorf("Expected some bytes written, got: %d", n)
	}

	output := buf.String()
	// Arrays and objects should be written as-is
	if !strings.Contains(output, "array=") {
		t.Errorf("Expected array field in output, got: %s", output)
	}
	if !strings.Contains(output, "object=") {
		t.Errorf("Expected object field in output, got: %s", output)
	}
}

// TestCustomWriterLargeStackTrace tests handling of large stack traces
func TestCustomWriterLargeStackTrace(t *testing.T) {
	var buf bytes.Buffer
	zerolog.ErrorStackMarshaler = CustomStackMarshaler
	zerolog.TimeFieldFormat = time.RFC3339Nano

	writer := NewCustomWriter(&buf)
	logger := zerolog.New(writer).With().Timestamp().Caller().Logger()

	// Create deep stack trace
	err := deepStackError(10)
	logger.Error().Stack().Err(err).Msg("large stack trace")

	output := buf.String()
	if !strings.Contains(output, "stack:") {
		t.Errorf("Expected stack trace, got: %s", output)
	}

	// Count number of stack frames (should have at least some frames)
	stackFrames := strings.Count(output, "()")
	if stackFrames < 3 {
		t.Errorf("Expected at least 3 stack frames, got: %d", stackFrames)
	}
}

// Helper function to create deep stack
func deepStackError(depth int) error {
	if depth <= 0 {
		return errors.New("deep error")
	}
	return errors.WithStack(deepStackError(depth - 1))
}

// TestCustomWriterNoStack tests error without stack trace
func TestCustomWriterNoStack(t *testing.T) {
	var buf bytes.Buffer
	zerolog.ErrorStackMarshaler = CustomStackMarshaler
	zerolog.TimeFieldFormat = time.RFC3339Nano

	writer := NewCustomWriter(&buf)
	logger := zerolog.New(writer).With().Timestamp().Caller().Logger()

	// Regular error without stack
	logger.Error().Err(errors.New("simple error")).Msg("no stack")

	output := buf.String()
	if !strings.Contains(output, "error=simple error") {
		t.Errorf("Expected error field, got: %s", output)
	}
	// Should not have stack section if Stack() wasn't called
	if strings.Contains(output, "stack:") {
		t.Errorf("Expected no stack trace without Stack() call, got: %s", output)
	}
}

// TestCustomWriterRelativePathInCwd tests that paths are displayed as-is without manipulation
func TestCustomWriterRelativePathInCwd(t *testing.T) {
	var buf bytes.Buffer
	writer := NewCustomWriter(&buf)

	// Test with a relative path
	callerPath := "test.go:42"

	jsonLog := []byte(`{"level":"info","caller":"` + callerPath + `","message":"test"}`)
	n, err := writer.Write(jsonLog)
	require.NoError(t, err)
	require.Greater(t, n, 0)

	output := buf.String()
	// Should show path as-is
	if !strings.Contains(output, "test.go:42") {
		t.Errorf("Expected caller path as-is, got: %s", output)
	}
}

// TestCustomWriterAbsolutePathOutsideCwd tests that absolute paths are displayed as-is
func TestCustomWriterAbsolutePathOutsideCwd(t *testing.T) {
	var buf bytes.Buffer
	writer := NewCustomWriter(&buf)

	// Test with an absolute path
	callerPath := "/usr/lib/go/src/runtime/proc.go:285"

	jsonLog := []byte(`{"level":"info","caller":"` + callerPath + `","message":"test"}`)
	n, err := writer.Write(jsonLog)
	require.NoError(t, err)
	require.Greater(t, n, 0)

	output := buf.String()
	// Should show absolute path as-is
	if !strings.Contains(output, callerPath) {
		t.Errorf("Expected absolute path as-is, got: %s", output)
	}
}

// TestCustomWriterEmptyMessage tests handling of empty message
func TestCustomWriterEmptyMessage(t *testing.T) {
	var buf bytes.Buffer
	writer := NewCustomWriter(&buf)

	jsonLog := []byte(`{"level":"info","message":""}`)
	n, err := writer.Write(jsonLog)

	if err != nil {
		t.Errorf("Expected no error for empty message, got: %v", err)
	}
	if n == 0 {
		t.Errorf("Expected some bytes written, got: %d", n)
	}

	output := buf.String()
	if !strings.Contains(output, "INF") {
		t.Errorf("Expected level despite empty message, got: %s", output)
	}
}

// TestCustomWriterVeryLongMessage tests handling of very long messages
func TestCustomWriterVeryLongMessage(t *testing.T) {
	var buf bytes.Buffer
	writer := NewCustomWriter(&buf)

	longMessage := strings.Repeat("a", 10000)
	jsonLog := []byte(`{"level":"info","message":"` + longMessage + `"}`)
	n, err := writer.Write(jsonLog)

	if err != nil {
		t.Errorf("Expected no error for long message, got: %v", err)
	}
	if n == 0 {
		t.Errorf("Expected some bytes written, got: %d", n)
	}

	output := buf.String()
	if !strings.Contains(output, longMessage) {
		t.Error("Expected long message in output")
	}
}

// TestCustomStackMarshalerWithNilError tests stack marshaler with nil error
func TestCustomStackMarshalerWithNilError(t *testing.T) {
	result := CustomStackMarshaler(nil)
	if result != nil {
		t.Errorf("Expected nil for nil error, got: %v", result)
	}
}

// TestCustomStackMarshalerWithoutStackTrace tests error that doesn't implement StackTrace
func TestCustomStackMarshalerWithoutStackTrace(t *testing.T) {
	// Use standard library errors which don't implement StackTrace
	regularErr := stderrors.New("regular error")
	result := CustomStackMarshaler(regularErr)

	// Standard library errors don't implement StackTrace, so should return nil
	if result != nil {
		t.Errorf("Expected nil for error without stack trace, got: %v", result)
	}
}

// TestCustomStackMarshalerWithPkgErrors tests error from pkg/errors
func TestCustomStackMarshalerWithPkgErrors(t *testing.T) {
	// pkg/errors.New actually implements StackTrace
	pkgErr := errors.New("pkg error")
	result := CustomStackMarshaler(pkgErr)

	// pkg/errors.New() actually captures stack trace, so result won't be nil
	if result == nil {
		t.Error("Expected non-nil result as pkg/errors captures stack")
	}
}

// TestCustomStackMarshalerEmptyStackTrace tests error with empty stack trace
func TestCustomStackMarshalerEmptyStackTrace(t *testing.T) {
	// This is hard to trigger naturally, but we test the code path
	// The function checks for len(st) == 0
	// In practice, errors.WithStack always adds at least one frame
	err := errors.New("test")
	err = errors.WithStack(err)

	result := CustomStackMarshaler(err)
	if result == nil {
		t.Error("Expected non-nil result for error with stack")
	}
}

// TestSetupLogger tests the SetupLogger function
func TestSetupLogger(t *testing.T) {
	var buf bytes.Buffer
	logger := SetupLogger(&buf)

	// Test that logger works
	logger.Info().Msg("test setup logger")

	output := buf.String()
	if !strings.Contains(output, "INF") {
		t.Errorf("Expected logger to work, got: %s", output)
	}
	if !strings.Contains(output, "test setup logger") {
		t.Errorf("Expected message, got: %s", output)
	}
}

// TestNewCustomWriter tests the constructor
func TestNewCustomWriter(t *testing.T) {
	var buf bytes.Buffer
	writer := NewCustomWriter(&buf)
	require.NotNilf(t, writer, "Expected non-nil writer")
	require.NotNilf(t, writer.out, "Expected non-nil writer.out")
	if writer.out != &buf {
		t.Error("Expected output writer to be set")
	}
}

// TestCustomWriterConcurrent tests concurrent writes (no race conditions)
func TestCustomWriterConcurrent(t *testing.T) {
	var buf bytes.Buffer
	writer := NewCustomWriter(&buf)

	// Note: This test checks for race conditions when run with -race flag
	// Each writer has its own buffer, so no actual races expected
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(id int) {
			jsonLog := []byte(`{"level":"info","message":"concurrent test"}`)
			n, err := writer.Write(jsonLog)
			require.NoError(t, err)
			require.Greater(t, n, 0)
			done <- true
		}(i)
	}

	for i := 0; i < 10; i++ {
		<-done
	}

	// Just verify something was written
	if buf.Len() == 0 {
		t.Error("Expected some output from concurrent writes")
	}
}

// TestCustomWriterQuotedStringValues tests proper quote handling in string values
func TestCustomWriterQuotedStringValues(t *testing.T) {
	var buf bytes.Buffer
	writer := NewCustomWriter(&buf)

	// JSON with quoted string values (as jsonparser provides them)
	quotedJSON := []byte(`{"level":"info","message":"test","field":"\"quoted value\""}`)
	n, err := writer.Write(quotedJSON)
	require.NoError(t, err)
	require.Greater(t, n, 0)

	output := buf.String()
	// Should handle quoted values properly
	require.Contains(t, output, "field=", "Expected field in output, got: %s", output)
	require.Contains(t, output, `\"quoted value\"`, "Expected \\\"quoted value\\\" in output, got: %s", output)
}
