package backup

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog/log"
)

// PerformanceMonitor tracks download performance and adjusts concurrency dynamically
type PerformanceMonitor struct {
	mu                   sync.RWMutex
	startTime            time.Time
	bytesDownloaded      int64
	lastBytesDownloaded  int64
	lastSpeedCheck       time.Time
	currentSpeed         float64 // bytes per second
	averageSpeed         float64 // bytes per second
	peakSpeed            float64 // bytes per second
	speedSamples         []float64
	maxSamples           int
	degradationThreshold float64 // threshold for detecting performance degradation
	degradationDetected  bool
	currentConcurrency   int32
	optimalConcurrency   int32
	maxConcurrency       int32
	minConcurrency       int32
	adjustmentInterval   time.Duration
	lastAdjustment       time.Time
	adjustmentCooldown   time.Duration
	performanceCallbacks []PerformanceCallback
}

// PerformanceCallback is called when performance changes are detected
type PerformanceCallback func(monitor *PerformanceMonitor, event PerformanceEvent)

// PerformanceEvent represents different performance events
type PerformanceEvent int

const (
	EventPerformanceDegradation PerformanceEvent = iota
	EventPerformanceImprovement
	EventConcurrencyAdjusted
	EventOptimalConcurrencyFound
)

// NewPerformanceMonitor creates a new performance monitor
func NewPerformanceMonitor(initialConcurrency, maxConcurrency int) *PerformanceMonitor {
	return &PerformanceMonitor{
		startTime:            time.Now(),
		lastSpeedCheck:       time.Now(),
		maxSamples:           20,  // Keep last 20 speed samples
		degradationThreshold: 0.3, // 30% degradation threshold
		currentConcurrency:   int32(initialConcurrency),
		optimalConcurrency:   int32(initialConcurrency),
		maxConcurrency:       int32(maxConcurrency),
		minConcurrency:       1,
		adjustmentInterval:   30 * time.Second, // Check every 30 seconds
		adjustmentCooldown:   60 * time.Second, // Wait 60 seconds between adjustments
		speedSamples:         make([]float64, 0, 20),
		performanceCallbacks: make([]PerformanceCallback, 0),
	}
}

// AddBytes records downloaded bytes
func (pm *PerformanceMonitor) AddBytes(bytes int64) {
	atomic.AddInt64(&pm.bytesDownloaded, bytes)
}

// UpdateSpeed calculates current speed and detects performance issues
func (pm *PerformanceMonitor) UpdateSpeed() {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	now := time.Now()
	timeSinceLastCheck := now.Sub(pm.lastSpeedCheck)

	// Only update if enough time has passed
	if timeSinceLastCheck < 5*time.Second {
		return
	}

	currentBytes := atomic.LoadInt64(&pm.bytesDownloaded)
	bytesDelta := currentBytes - pm.lastBytesDownloaded

	if bytesDelta > 0 && timeSinceLastCheck > 0 {
		pm.currentSpeed = float64(bytesDelta) / timeSinceLastCheck.Seconds()

		// Add to speed samples
		pm.speedSamples = append(pm.speedSamples, pm.currentSpeed)
		if len(pm.speedSamples) > pm.maxSamples {
			pm.speedSamples = pm.speedSamples[1:]
		}

		// Update peak speed
		if pm.currentSpeed > pm.peakSpeed {
			pm.peakSpeed = pm.currentSpeed
		}

		// Calculate average speed
		if len(pm.speedSamples) > 0 {
			sum := 0.0
			for _, speed := range pm.speedSamples {
				sum += speed
			}
			pm.averageSpeed = sum / float64(len(pm.speedSamples))
		}

		// Detect performance degradation
		pm.detectPerformanceDegradation()

		// Log performance metrics
		log.Debug().
			Float64("current_speed_mbps", pm.currentSpeed/(1024*1024)).
			Float64("average_speed_mbps", pm.averageSpeed/(1024*1024)).
			Float64("peak_speed_mbps", pm.peakSpeed/(1024*1024)).
			Int32("concurrency", pm.currentConcurrency).
			Bool("degradation_detected", pm.degradationDetected).
			Msg("performance_monitor_update")
	}

	pm.lastBytesDownloaded = currentBytes
	pm.lastSpeedCheck = now
}

// detectPerformanceDegradation checks if performance has degraded significantly
func (pm *PerformanceMonitor) detectPerformanceDegradation() {
	if pm.peakSpeed == 0 || len(pm.speedSamples) < 5 {
		return
	}

	// Calculate recent average (last 5 samples)
	recentSamples := pm.speedSamples
	if len(recentSamples) > 5 {
		recentSamples = recentSamples[len(recentSamples)-5:]
	}

	recentAverage := 0.0
	for _, speed := range recentSamples {
		recentAverage += speed
	}
	recentAverage = recentAverage / float64(len(recentSamples))

	// Check for degradation compared to peak
	degradationRatio := (pm.peakSpeed - recentAverage) / pm.peakSpeed
	wasDetected := pm.degradationDetected
	pm.degradationDetected = degradationRatio > pm.degradationThreshold

	// Notify callbacks if degradation status changed
	if pm.degradationDetected && !wasDetected {
		pm.notifyCallbacks(EventPerformanceDegradation)
	} else if !pm.degradationDetected && wasDetected {
		pm.notifyCallbacks(EventPerformanceImprovement)
	}
}

// ShouldAdjustConcurrency determines if concurrency should be adjusted
func (pm *PerformanceMonitor) ShouldAdjustConcurrency() bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	now := time.Now()

	// Check if enough time has passed since last adjustment
	if now.Sub(pm.lastAdjustment) < pm.adjustmentCooldown {
		return false
	}

	// Check if enough time has passed since start for reliable data
	if now.Sub(pm.startTime) < pm.adjustmentInterval {
		return false
	}

	// Need enough samples for reliable decision
	return len(pm.speedSamples) >= 5
}

// AdjustConcurrency dynamically adjusts concurrency based on performance
func (pm *PerformanceMonitor) AdjustConcurrency() int32 {
	if !pm.ShouldAdjustConcurrency() {
		return pm.GetCurrentConcurrency()
	}

	pm.mu.Lock()
	defer pm.mu.Unlock()

	oldConcurrency := pm.currentConcurrency
	newConcurrency := pm.currentConcurrency

	if pm.degradationDetected {
		// Performance is degrading, reduce concurrency
		reduction := int32(math.Max(1, float64(pm.currentConcurrency)*0.2))
		newConcurrency = pm.currentConcurrency - reduction

		if newConcurrency < pm.minConcurrency {
			newConcurrency = pm.minConcurrency
		}

		log.Info().
			Int32("old_concurrency", oldConcurrency).
			Int32("new_concurrency", newConcurrency).
			Float64("degradation_ratio", (pm.peakSpeed-pm.averageSpeed)/pm.peakSpeed).
			Msg("reducing_concurrency_due_to_performance_degradation")

	} else if pm.currentSpeed > pm.averageSpeed*1.2 && pm.currentConcurrency < pm.maxConcurrency {
		// Performance is good, try increasing concurrency slightly
		increase := int32(math.Max(1, float64(pm.currentConcurrency)*0.1))
		newConcurrency = pm.currentConcurrency + increase

		if newConcurrency > pm.maxConcurrency {
			newConcurrency = pm.maxConcurrency
		}

		log.Info().
			Int32("old_concurrency", oldConcurrency).
			Int32("new_concurrency", newConcurrency).
			Float64("current_speed_mbps", pm.currentSpeed/(1024*1024)).
			Msg("increasing_concurrency_due_to_good_performance")
	}

	if newConcurrency != oldConcurrency {
		pm.currentConcurrency = newConcurrency
		pm.lastAdjustment = time.Now()
		pm.notifyCallbacks(EventConcurrencyAdjusted)
	}

	return pm.currentConcurrency
}

// GetCurrentConcurrency returns the current concurrency level
func (pm *PerformanceMonitor) GetCurrentConcurrency() int32 {
	return atomic.LoadInt32(&pm.currentConcurrency)
}

// GetMetrics returns current performance metrics
func (pm *PerformanceMonitor) GetMetrics() PerformanceMetrics {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	totalBytes := atomic.LoadInt64(&pm.bytesDownloaded)
	totalDuration := time.Since(pm.startTime)

	return PerformanceMetrics{
		TotalBytes:          totalBytes,
		TotalDuration:       totalDuration,
		CurrentSpeed:        pm.currentSpeed,
		AverageSpeed:        pm.averageSpeed,
		PeakSpeed:           pm.peakSpeed,
		CurrentConcurrency:  pm.GetCurrentConcurrency(),
		OptimalConcurrency:  pm.optimalConcurrency,
		DegradationDetected: pm.degradationDetected,
		SpeedSamples:        len(pm.speedSamples),
	}
}

// PerformanceMetrics contains performance statistics
type PerformanceMetrics struct {
	TotalBytes          int64
	TotalDuration       time.Duration
	CurrentSpeed        float64
	AverageSpeed        float64
	PeakSpeed           float64
	CurrentConcurrency  int32
	OptimalConcurrency  int32
	DegradationDetected bool
	SpeedSamples        int
}

// String returns a human-readable representation of the metrics
func (pm PerformanceMetrics) String() string {
	return fmt.Sprintf(
		"Performance: %.2f MB/s current, %.2f MB/s avg, %.2f MB/s peak, %d concurrency, %s total, %v degraded",
		pm.CurrentSpeed/(1024*1024),
		pm.AverageSpeed/(1024*1024),
		pm.PeakSpeed/(1024*1024),
		pm.CurrentConcurrency,
		pm.TotalDuration.Round(time.Second),
		pm.DegradationDetected,
	)
}

// AddCallback adds a performance callback
func (pm *PerformanceMonitor) AddCallback(callback PerformanceCallback) {
	pm.mu.Lock()
	defer pm.mu.Unlock()
	pm.performanceCallbacks = append(pm.performanceCallbacks, callback)
}

// notifyCallbacks notifies all registered callbacks
func (pm *PerformanceMonitor) notifyCallbacks(event PerformanceEvent) {
	for _, callback := range pm.performanceCallbacks {
		go callback(pm, event)
	}
}

// StartMonitoring starts the performance monitoring loop
func (pm *PerformanceMonitor) StartMonitoring(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pm.UpdateSpeed()
		}
	}
}
