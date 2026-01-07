using System;
using System.Collections.Generic;

namespace FabricQueryDemos.Models
{
    // ============================================================================
    // Data Models
    // ============================================================================
    // These classes represent the structure of test data and results.
    // They support CSV serialization via CsvHelper for data export.
    // ============================================================================

    // Represents a single query execution with timing and metadata
    public class QueryRun
    {
        public int IterationSize { get; set; }
        public int RunNumber { get; set; }
        public string Client { get; set; }
        public long ElapsedMilliseconds { get; set; }
        public int RowCount { get; set; }
        public long PayloadBytes { get; set; }
        public DateTime ExecutedAt { get; set; }
    }

    // Aggregated performance statistics for a specific iteration size and client.
    // Calculated from multiple QueryRun instances to provide statistical significance.
    public class IterationStatistics
    {
        public int IterationSize { get; set; }
        public string Client { get; set; }
        public List<long> RawLatencies { get; set; } = new();  // All measured latencies for this group
        public long Mean { get; set; }                          // Average latency (ms)
        public double StdDev { get; set; }                      // Standard deviation (consistency metric)
        public long Min { get; set; }                           // Best case latency (ms)
        public long Max { get; set; }                           // Worst case latency (ms)
        public long P50 { get; set; }                           // Median - 50% of queries faster
        public long P95 { get; set; }                           // 95th percentile - typical SLA target
        public long P99 { get; set; }                           // 99th percentile - tail latency
        public double ThroughputRowsPerSecond { get; set; }
        public int TotalRowsRetrieved { get; set; }
        public long TotalPayloadBytes { get; set; }
    }

    // Complete test execution results including all runs and calculated statistics.
    // This is the root object that contains all test data and is serialized to CSV.
    public class TestResult
    {
        public List<QueryRun> Runs { get; set; } = new();              // Raw individual query executions
        public List<IterationStatistics> Statistics { get; set; } = new();  // Aggregated statistics per group
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public bool Success { get; set; }
        public string ErrorMessage { get; set; }
    }
}
