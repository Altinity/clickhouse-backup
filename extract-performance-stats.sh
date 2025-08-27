#!/bin/bash
# extract-performance-stats.sh - Extract and analyze performance statistics from logs

set -e

# Default values
LOG_FILE="${1:-/var/log/clickhouse-backup.log}"
OUTPUT_DIR="./performance-analysis"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)

# Create output directory
mkdir -p "$OUTPUT_DIR"

echo "=== ClickHouse Backup Performance Statistics Extractor ==="
echo "Log file: $LOG_FILE"
echo "Output directory: $OUTPUT_DIR"
echo

# Check if log file exists
if [[ ! -f "$LOG_FILE" ]]; then
    echo "Error: Log file not found: $LOG_FILE"
    echo "Usage: $0 [log_file_path]"
    exit 1
fi

# Check if jq is installed
if ! command -v jq &> /dev/null; then
    echo "Error: jq is required but not installed. Please install jq first."
    echo "Ubuntu/Debian: sudo apt-get install jq"
    echo "CentOS/RHEL: sudo yum install jq"
    echo "macOS: brew install jq"
    exit 1
fi

# Extract performance-related entries
PERFORMANCE_LOG="${OUTPUT_DIR}/performance-events-${TIMESTAMP}.jsonl"
SUMMARY_FILE="${OUTPUT_DIR}/performance-summary-${TIMESTAMP}.txt"
METRICS_CSV="${OUTPUT_DIR}/performance-metrics-${TIMESTAMP}.csv"

echo "Extracting performance events..."
grep -E "(performance|degradation|concurrency)" "$LOG_FILE" | \
grep -E "(performance_monitor_update|degradation detected|concurrency adjusted|performance metrics)" > "$PERFORMANCE_LOG" || true

if [[ ! -s "$PERFORMANCE_LOG" ]]; then
    echo "Warning: No performance events found in the log file."
    echo "Make sure you're running clickhouse-backup with debug logging enabled."
    echo "Example: clickhouse-backup --log-level=debug restore my-backup"
    exit 1
fi

echo "Performance events saved to: $PERFORMANCE_LOG"

# Generate summary report
echo "Generating performance summary..."
{
    echo "=== CLICKHOUSE BACKUP PERFORMANCE ANALYSIS ==="
    echo "Generated: $(date)"
    echo "Log file: $LOG_FILE"
    echo "Analysis period: $(head -1 "$PERFORMANCE_LOG" | jq -r '.time // "N/A"') to $(tail -1 "$PERFORMANCE_LOG" | jq -r '.time // "N/A"')"
    echo
    
    echo "=== EVENT SUMMARY ==="
    total_events=$(wc -l < "$PERFORMANCE_LOG")
    degradation_events=$(grep -c "degradation detected" "$PERFORMANCE_LOG" || echo "0")
    improvement_events=$(grep -c "performance improvement" "$PERFORMANCE_LOG" || echo "0") 
    concurrency_adjustments=$(grep -c "concurrency adjusted" "$PERFORMANCE_LOG" || echo "0")
    monitor_updates=$(grep -c "performance_monitor_update" "$PERFORMANCE_LOG" || echo "0")
    
    echo "Total performance events: $total_events"
    echo "Performance degradations: $degradation_events"
    echo "Performance improvements: $improvement_events"
    echo "Concurrency adjustments: $concurrency_adjustments"
    echo "Monitor updates: $monitor_updates"
    echo
    
    echo "=== SPEED ANALYSIS ==="
    # Extract speed metrics
    peak_speeds=$(grep "performance_monitor_update\|performance metrics" "$PERFORMANCE_LOG" | \
                 jq -r '.peak_speed_mbps // empty' | \
                 awk '{if($1>max) max=$1} END {print (max ? max : "N/A")}')
    
    avg_speeds=$(grep "performance_monitor_update\|performance metrics" "$PERFORMANCE_LOG" | \
                jq -r '.average_speed_mbps // empty' | \
                awk '{sum+=$1; count++} END {print (count ? sum/count : "N/A")}')
    
    current_speeds=$(grep "performance_monitor_update" "$PERFORMANCE_LOG" | \
                    jq -r '.current_speed_mbps // empty' | \
                    awk '{sum+=$1; count++} END {print (count ? sum/count : "N/A")}')
    
    echo "Peak speed achieved: ${peak_speeds} MB/s"
    echo "Average speed overall: ${avg_speeds} MB/s"  
    echo "Average current speed: ${current_speeds} MB/s"
    echo
    
    echo "=== CONCURRENCY ANALYSIS ==="
    initial_concurrency=$(grep "performance_monitor_update" "$PERFORMANCE_LOG" | head -1 | jq -r '.concurrency // "N/A"')
    final_concurrency=$(grep "performance_monitor_update\|performance metrics" "$PERFORMANCE_LOG" | tail -1 | jq -r '.final_concurrency // .concurrency // "N/A"')
    max_concurrency=$(grep "performance_monitor_update\|concurrency adjusted" "$PERFORMANCE_LOG" | \
                     jq -r '.concurrency // empty' | \
                     awk '{if($1>max) max=$1} END {print (max ? max : "N/A")}')
    min_concurrency=$(grep "performance_monitor_update\|concurrency adjusted" "$PERFORMANCE_LOG" | \
                     jq -r '.concurrency // empty' | \
                     awk 'BEGIN{min=999999} {if($1<min) min=$1} END {print (min==999999 ? "N/A" : min)}')
    
    echo "Initial concurrency: $initial_concurrency"
    echo "Final concurrency: $final_concurrency"
    echo "Maximum concurrency: $max_concurrency"
    echo "Minimum concurrency: $min_concurrency"
    echo
    
    echo "=== DEGRADATION ANALYSIS ==="
    if [[ $degradation_events -gt 0 ]]; then
        echo "Degradation rate: $(echo "scale=2; $degradation_events * 100 / $monitor_updates" | bc)% of monitoring intervals"
        
        # Find worst degradation
        worst_degradation=$(grep "degradation detected" "$PERFORMANCE_LOG" | \
                           jq -r '(.peak_speed_mbps - .current_speed_mbps) / .peak_speed_mbps * 100' | \
                           awk '{if($1>max) max=$1} END {print (max ? max : "N/A")}')
        echo "Worst degradation: ${worst_degradation}% speed drop"
        
        # Recovery time estimation
        echo "Recovery capability: $improvement_events recoveries from $degradation_events degradations"
    else
        echo "No performance degradations detected! 🎉"
    fi
    echo
    
    echo "=== TABLE-SPECIFIC ANALYSIS ==="
    grep "performance" "$PERFORMANCE_LOG" | jq -r '.table // "unknown"' | sort | uniq -c | sort -nr | head -10
    echo
    
    echo "=== STORAGE TYPE ANALYSIS ==="
    # Try to extract storage information if available
    storage_types=$(grep "performance" "$PERFORMANCE_LOG" | jq -r '.disk // .storage_type // "unknown"' | sort | uniq -c | sort -nr)
    if [[ -n "$storage_types" ]]; then
        echo "$storage_types"
    else
        echo "Storage type information not available in logs"
    fi
    
} > "$SUMMARY_FILE"

echo "Performance summary saved to: $SUMMARY_FILE"

# Generate CSV for analysis tools
echo "Generating CSV metrics..."
{
    echo "timestamp,event_type,table,current_speed_mbps,average_speed_mbps,peak_speed_mbps,concurrency,degradation_detected,disk"
    
    grep "performance_monitor_update\|degradation detected\|concurrency adjusted\|performance metrics" "$PERFORMANCE_LOG" | \
    jq -r '[
        .time // "",
        (.message | if contains("degradation") then "degradation" 
                   elif contains("concurrency") then "concurrency_adjustment"
                   elif contains("metrics") then "completion"
                   else "update" end),
        .table // "",
        .current_speed_mbps // "",
        .average_speed_mbps // .avg_speed_mbps // "",
        .peak_speed_mbps // "",
        .concurrency // .final_concurrency // "",
        .degradation_detected // "",
        .disk // ""
    ] | @csv'
} > "$METRICS_CSV"

echo "CSV metrics saved to: $METRICS_CSV"

# Display summary
echo
echo "=== ANALYSIS COMPLETE ==="
cat "$SUMMARY_FILE"

echo
echo "=== FILES GENERATED ==="
echo "📄 Raw events: $PERFORMANCE_LOG"
echo "📊 Summary report: $SUMMARY_FILE"  
echo "📈 CSV metrics: $METRICS_CSV"
echo
echo "You can use these files for further analysis with tools like:"
echo "- Excel/LibreOffice (open the CSV file)"
echo "- Python pandas: pd.read_csv('$METRICS_CSV')"
echo "- R: read.csv('$METRICS_CSV')"
echo "- Grafana/Prometheus for visualization"