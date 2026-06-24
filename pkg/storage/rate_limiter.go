package storage

import (
	"github.com/Altinity/clickhouse-backup/v2/pkg/storage/bwlimit"
	"github.com/rs/zerolog/log"
)

// UploadLimiter returns the shared upload rate limiter for this destination,
// creating (or recreating, on a rate change) it lazily. It returns nil when
// throttling is disabled (maxBytesPerSecond == 0). The limiter is shared across
// all concurrent upload workers so the configured rate is an aggregate cap.
func (bd *BackupDestination) UploadLimiter(maxBytesPerSecond uint64) *bwlimit.Limiter {
	if maxBytesPerSecond == 0 {
		return nil
	}
	bd.limiterMu.Lock()
	defer bd.limiterMu.Unlock()
	if !bd.uploadRateLimiter.SameRate(maxBytesPerSecond) {
		log.Debug().Msgf("bwlimit: upload rate limiter active, %d bytes/s aggregate", maxBytesPerSecond)
		bd.uploadRateLimiter = bwlimit.New(maxBytesPerSecond)
	}
	return bd.uploadRateLimiter
}

// DownloadLimiter mirrors UploadLimiter for the download direction.
func (bd *BackupDestination) DownloadLimiter(maxBytesPerSecond uint64) *bwlimit.Limiter {
	if maxBytesPerSecond == 0 {
		return nil
	}
	bd.limiterMu.Lock()
	defer bd.limiterMu.Unlock()
	if !bd.downloadRateLimiter.SameRate(maxBytesPerSecond) {
		log.Debug().Msgf("bwlimit: download rate limiter active, %d bytes/s aggregate", maxBytesPerSecond)
		bd.downloadRateLimiter = bwlimit.New(maxBytesPerSecond)
	}
	return bd.downloadRateLimiter
}
