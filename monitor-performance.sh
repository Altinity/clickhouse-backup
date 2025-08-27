#!/bin/bash
# monitor-performance.sh - Real-time performance monitoring for clickhouse-backup

echo "=== ClickHouse Backup Performance Monitor ==="
echo "Monitoring performance degradation and statistics..."
echo "Press Ctrl+C to stop monitoring"
echo

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to parse and display performance metrics
parse_performance_logs() {
    while IFS= read -r line; do
        # Check if the line contains JSON (performance logs)
        if echo "$line" | jq empty 2>/dev/null; then
            
            # Performance degradation detected
            if echo "$line" | jq -e '.message | contains("performance degradation detected")' >/dev/null 2>&1; then
                echo -e "\n${RED}🚨 PERFORMANCE DEGRADATION DETECTED:${NC}"
                current_speed=$(echo "$line" | jq -r '.current_speed_mbps // "N/A"')
                avg_speed=$(echo "$line" | jq -r '.average_speed_mbps // "N/A"')
                peak_speed=$(echo "$line" | jq -r '.peak_speed_mbps // "N/A"')
                concurrency=$(echo "$line" | jq -r '.concurrency // "N/A"')
                table=$(echo "$line" | jq -r '.table // "N/A"')
                echo "  📉 Speed dropped to ${current_speed}MB/s (avg: ${avg_speed}MB/s, peak: ${peak_speed}MB/s)"
                echo "  📋 Table: ${table}, Concurrency: ${concurrency}"
                
            # Concurrency adjusted
            elif echo "$line" | jq -e '.message | contains("concurrency adjusted")' >/dev/null 2>&1; then
                echo -e "\n${GREEN}⚡ CONCURRENCY ADJUSTED:${NC}"
                concurrency=$(echo "$line" | jq -r '.concurrency // "N/A"')
                speed=$(echo "$line" | jq -r '.speed_mbps // "N/A"')
                table=$(echo "$line" | jq -r '.table // "N/A"')
                echo "  🔧 New concurrency: ${concurrency}, Current speed: ${speed}MB/s"
                echo "  📋 Table: ${table}"
                
            # Performance monitor update
            elif echo "$line" | jq -e '.message | contains("performance_monitor_update")' >/dev/null 2>&1; then
                echo -e "\n${BLUE}📊 PERFORMANCE UPDATE:${NC}"
                current_speed=$(echo "$line" | jq -r '.current_speed_mbps // "N/A"')
                avg_speed=$(echo "$line" | jq -r '.average_speed_mbps // "N/A"')
                peak_speed=$(echo "$line" | jq -r '.peak_speed_mbps // "N/A"')
                concurrency=$(echo "$line" | jq -r '.concurrency // "N/A"')
                degradation=$(echo "$line" | jq -r '.degradation_detected // "N/A"')
                echo "  📈 Current: ${current_speed}MB/s | Avg: ${avg_speed}MB/s | Peak: ${peak_speed}MB/s"
                echo "  🔀 Concurrency: ${concurrency} | Degraded: ${degradation}"
                
            # Final performance metrics
            elif echo "$line" | jq -e '.message | contains("performance metrics")' >/dev/null 2>&1; then
                echo -e "\n${YELLOW}🏁 DOWNLOAD COMPLETED WITH PERFORMANCE METRICS:${NC}"
                disk=$(echo "$line" | jq -r '.disk // "N/A"')
                duration=$(echo "$line" | jq -r '.duration // "N/A"')
                size=$(echo "$line" | jq -r '.size // "N/A"')
                avg_speed=$(echo "$line" | jq -r '.avg_speed_mbps // "N/A"')
                peak_speed=$(echo "$line" | jq -r '.peak_speed_mbps // "N/A"')
                final_concurrency=$(echo "$line" | jq -r '.final_concurrency // "N/A"')
                degradation=$(echo "$line" | jq -r '.degradation_detected // "N/A"')
                echo "  💾 Disk: ${disk} | Duration: ${duration} | Size: ${size}"
                echo "  📊 Avg Speed: ${avg_speed}MB/s | Peak: ${peak_speed}MB/s"
                echo "  🔀 Final Concurrency: ${final_concurrency} | Had Degradation: ${degradation}"
            fi
        fi
    done
}

# Check if log file is specified as argument
LOG_FILE="${1:-/var/log/clickhouse-backup.log}"

if [[ ! -f "$LOG_FILE" ]]; then
    echo "Log file not found: $LOG_FILE"
    echo "Usage: $0 [log_file_path]"
    echo "Trying alternative log locations..."
    
    # Try common log locations
    POSSIBLE_LOGS=(
        "/var/log/clickhouse-backup.log"
        "/tmp/clickhouse-backup.log"
        "./clickhouse-backup.log"
        "/home/$USER/.clickhouse-backup/clickhouse-backup.log"
    )
    
    for log in "${POSSIBLE_LOGS[@]}"; do
        if [[ -f "$log" ]]; then
            echo "Found log file: $log"
            LOG_FILE="$log"
            break
        fi
    done
    
    if [[ ! -f "$LOG_FILE" ]]; then
        echo "No log file found. Please specify the correct path or run clickhouse-backup with logging enabled."
        echo "Example: clickhouse-backup restore my-backup 2>&1 | tee clickhouse-backup.log"
        exit 1
    fi
fi

echo "Monitoring log file: $LOG_FILE"
echo "Looking for performance metrics..."
echo

# Monitor the log file
tail -f "$LOG_FILE" | parse_performance_logs