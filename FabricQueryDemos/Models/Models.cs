using System;
using System.Collections.Generic;

namespace FabricQueryDemos.Models
{

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

    public class IterationStatistics
    {
        public int IterationSize { get; set; }
        public string Client { get; set; }
        public List<long> RawLatencies { get; set; } = new();
        public long Mean { get; set; }
        public double StdDev { get; set; }
        public long Min { get; set; }
        public long Max { get; set; }
        public long P50 { get; set; }
        public long P95 { get; set; }
        public long P99 { get; set; }
        public double ThroughputRowsPerSecond { get; set; }
        public int TotalRowsRetrieved { get; set; }
        public long TotalPayloadBytes { get; set; }
    }

    public class TestResult
    {
        public List<QueryRun> Runs { get; set; } = new();
        public List<IterationStatistics> Statistics { get; set; } = new();
        public DateTime StartTime { get; set; }
        public DateTime EndTime { get; set; }
        public bool Success { get; set; }
        public string ErrorMessage { get; set; }
    }

    public class GraphQLResponse<T>
    {
        public T Data { get; set; }
        public List<GraphQLError> Errors { get; set; }
    }

    public class GraphQLError
    {
        public string Message { get; set; }
    }


}
