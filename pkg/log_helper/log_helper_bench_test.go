package log_helper

import (
	"io"
	"testing"

	"github.com/pkg/errors"
	"github.com/rs/zerolog"
)

// discardWriter is an io.Writer that discards all data
type discardWriter struct{}

func (d discardWriter) Write(p []byte) (n int, err error) {
	return len(p), nil
}

// BenchmarkCustomWriter benchmarks our custom writer
func BenchmarkCustomWriter(b *testing.B) {
	zerolog.ErrorStackMarshaler = CustomStackMarshaler
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"

	writer := NewCustomWriter(discardWriter{})
	logger := zerolog.New(writer).With().Timestamp().Caller().Logger()

	err := errors.New("test error")
	err = errors.WithStack(err)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		logger.Error().Stack().Err(err).Str("userId", "12345").Int("count", 42).Msg("benchmark test")
	}
}

// BenchmarkConsoleWriter benchmarks zerolog's built-in ConsoleWriter
func BenchmarkConsoleWriter(b *testing.B) {
	zerolog.ErrorStackMarshaler = CustomStackMarshaler
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"

	consoleWriter := zerolog.ConsoleWriter{
		Out:        discardWriter{},
		NoColor:    true,
		TimeFormat: "2006-01-02 15:04:05.000",
	}

	logger := zerolog.New(consoleWriter).With().Timestamp().Caller().Logger()

	err := errors.New("test error")
	err = errors.WithStack(err)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		logger.Error().Stack().Err(err).Str("userId", "12345").Int("count", 42).Msg("benchmark test")
	}
}

// BenchmarkCustomWriterSimple benchmarks custom writer with simple log (no error, no stack)
func BenchmarkCustomWriterSimple(b *testing.B) {
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"

	writer := NewCustomWriter(discardWriter{})
	logger := zerolog.New(writer).With().Timestamp().Caller().Logger()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		logger.Info().Str("userId", "12345").Int("count", 42).Msg("simple log message")
	}
}

// BenchmarkConsoleWriterSimple benchmarks ConsoleWriter with simple log
func BenchmarkConsoleWriterSimple(b *testing.B) {
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"

	consoleWriter := zerolog.ConsoleWriter{
		Out:        discardWriter{},
		NoColor:    true,
		TimeFormat: "2006-01-02 15:04:05.000",
	}

	logger := zerolog.New(consoleWriter).With().Timestamp().Caller().Logger()

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		logger.Info().Str("userId", "12345").Int("count", 42).Msg("simple log message")
	}
}

// BenchmarkCustomWriterComplex benchmarks custom writer with complex log (many fields)
func BenchmarkCustomWriterComplex(b *testing.B) {
	zerolog.ErrorStackMarshaler = CustomStackMarshaler
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"

	writer := NewCustomWriter(discardWriter{})
	logger := zerolog.New(writer).With().Timestamp().Caller().Logger()

	err := errors.New("test error")
	err = errors.WithStack(err)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		logger.Error().
			Stack().
			Err(err).
			Str("userId", "12345").
			Int("count", 42).
			Str("service", "api").
			Str("host", "localhost").
			Int("port", 8080).
			Bool("success", false).
			Float64("duration", 123.456).
			Msg("complex benchmark test")
	}
}

// BenchmarkConsoleWriterComplex benchmarks ConsoleWriter with complex log
func BenchmarkConsoleWriterComplex(b *testing.B) {
	zerolog.ErrorStackMarshaler = CustomStackMarshaler
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"

	consoleWriter := zerolog.ConsoleWriter{
		Out:        discardWriter{},
		NoColor:    true,
		TimeFormat: "2006-01-02 15:04:05.000",
	}

	logger := zerolog.New(consoleWriter).With().Timestamp().Caller().Logger()

	err := errors.New("test error")
	err = errors.WithStack(err)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		logger.Error().
			Stack().
			Err(err).
			Str("userId", "12345").
			Int("count", 42).
			Str("service", "api").
			Str("host", "localhost").
			Int("port", 8080).
			Bool("success", false).
			Float64("duration", 123.456).
			Msg("complex benchmark test")
	}
}

// BenchmarkJSONWriter benchmarks raw JSON output (baseline)
func BenchmarkJSONWriter(b *testing.B) {
	zerolog.ErrorStackMarshaler = CustomStackMarshaler
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"

	logger := zerolog.New(discardWriter{}).With().Timestamp().Caller().Logger()

	err := errors.New("test error")
	err = errors.WithStack(err)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		logger.Error().Stack().Err(err).Str("userId", "12345").Int("count", 42).Msg("benchmark test")
	}
}

// Benchmark with real io.Writer (os.Stderr equivalent)
func BenchmarkCustomWriterWithRealWriter(b *testing.B) {
	zerolog.ErrorStackMarshaler = CustomStackMarshaler
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"

	// Use io.Discard which is more realistic than our custom discardWriter
	writer := NewCustomWriter(io.Discard)
	logger := zerolog.New(writer).With().Timestamp().Caller().Logger()

	err := errors.New("test error")
	err = errors.WithStack(err)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		logger.Error().Stack().Err(err).Str("userId", "12345").Int("count", 42).Msg("benchmark test")
	}
}

func BenchmarkConsoleWriterWithRealWriter(b *testing.B) {
	zerolog.ErrorStackMarshaler = CustomStackMarshaler
	zerolog.TimeFieldFormat = "2006-01-02 15:04:05.000"

	consoleWriter := zerolog.ConsoleWriter{
		Out:        io.Discard,
		NoColor:    true,
		TimeFormat: "2006-01-02 15:04:05.000",
	}

	logger := zerolog.New(consoleWriter).With().Timestamp().Caller().Logger()

	err := errors.New("test error")
	err = errors.WithStack(err)

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		logger.Error().Stack().Err(err).Str("userId", "12345").Int("count", 42).Msg("benchmark test")
	}
}
