# # Fabric Query Test Engine

A .NET console application for benchmarking and comparing query execution across three Fabric endpoints: **KQL (Kusto Query Language)**, **Lakehouse SQL**, and **GraphQL**. The engine runs the same query with different row limits (10, 100, 1000, 10000 rows) and executes each iteration 5 times to capture latency statistics.

## Features

- **Multi-Endpoint Testing**: Execute queries against KQL, SQL, and GraphQL endpoints simultaneously
- **Iteration-Based Benchmarking**: Test with 4 different row counts to measure scaling behavior
- **Statistical Analysis**: Calculate mean, standard deviation, min, max, and percentiles (p50, p95, p99)
- **Multiple Runs**: Execute each test iteration 5 times to ensure statistical reliability
- **Raw & Aggregate Metrics**: Report all 5 raw latencies plus computed statistics
- **Interactive Authentication**: Uses browser-based login for secure credential handling
- **Multi-Format Output**: Console report, JSON, and CSV exports

## Prerequisites

- .NET 8.0 or later
- Access to Fabric Eventhouse with mxchip_silver table
- Access to Lakehouse SQL endpoint
- Access to Fabric GraphQL API endpoint
- Interactive browser for authentication

## Configuration

Edit `appsettings.json` to set your Fabric endpoints:

```json
{
  "Endpoints": {
    "KqlClusterUri": "https://YOUR_CLUSTER.kusto.windows.net",
    "KqlDatabase": "YOUR_DATABASE_NAME",
    "SqlServerEndpoint": "YOUR_LAKEHOUSE_ENDPOINT.datawarehouse.fabric.microsoft.com",
    "SqlDatabase": "YOUR_DATABASE_NAME",
    "GraphQLEndpoint": "https://YOUR_WORKSPACE_ID.zfa.graphql.fabric.microsoft.com/v1/workspaces/YOUR_WORKSPACE_ID/graphqlapis/YOUR_GRAPHQL_API_ID/graphql"
  },
  "TestConfiguration": {
    "IterationSizes": [ 10, 100, 1000, 10000 ],
    "RunsPerIteration": 5
  }
}
```

### Getting Your Endpoints

1. **KQL Cluster URI**: Available in your Fabric workspace settings
2. **SQL Server Endpoint**: Found in the Lakehouse SQL settings
3. **GraphQL Endpoint**: Available in the GraphQL API settings

## Running the Application

```bash
# Build the application
dotnet build

# Run the tests
dotnet run
```

When prompted, authenticate using your Microsoft account. The application will launch a browser window for interactive authentication.

## Output

The application generates three types of reports:

### Console Output
Real-time progress and summary statistics displayed in the terminal:
```
--- Testing with 10 rows ---
  Running KQL...
    Run 1: 234ms, 10 rows
    Run 2: 245ms, 10 rows
    ...
  Running SQL...
    Run 1: 156ms, 10 rows
    ...
  Running GraphQL...
    Run 1: 312ms, 10 rows
    ...

=== SUMMARY REPORT ===

KQL - 10 rows:
  Raw Latencies (ms): 234, 245, 238, 241, 239
  Mean:  239ms
  StdDev: 3.74
  Min:   234ms
  Max:   245ms
  P50:   239ms
  P95:   245ms
  P99:   245ms
  Throughput: 15.90 rows/sec
  Total Rows: 50
  Total Payload: 12500 bytes
```

### JSON Report
Detailed results saved to `results/report.json`:
```json
{
  "Runs": [
    {
      "IterationSize": 10,
      "RunNumber": 1,
      "Client": "KQL",
      "ElapsedMilliseconds": 234,
      "RowCount": 10,
      "PayloadBytes": 2500,
      "ExecutedAt": "2026-01-06T10:30:45Z"
    }
  ],
  "Statistics": [
    {
      "IterationSize": 10,
      "Client": "KQL",
      "RawLatencies": [234, 245, 238, 241, 239],
      "Mean": 239,
      "StdDev": 3.74,
      "Min": 234,
      "Max": 245,
      "P50": 239,
      "P95": 245,
      "P99": 245,
      "ThroughputRowsPerSecond": 15.9,
      "TotalRowsRetrieved": 50,
      "TotalPayloadBytes": 12500
    }
  ]
}
```

### CSV Report
Raw run data saved to `results/results.csv` for further analysis in Excel or other tools.

## Understanding the Metrics

- **Raw Latencies**: All 5 individual execution times in milliseconds
- **Mean**: Average latency across 5 runs
- **StdDev**: Standard deviation indicating consistency
- **Min/Max**: Best and worst-case latencies
- **P50, P95, P99**: Percentile latencies (50th, 95th, 99th percentile)
- **Throughput**: Average rows retrieved per second
- **Payload Bytes**: Estimated size of data transferred

## Troubleshooting

### Authentication Issues
- Ensure you have internet access and can authenticate via Microsoft
- Check that your Fabric workspace has appropriate access permissions
- For service principal auth, modify the credential in the client files

### Connection Timeouts
- Verify endpoints are correct in `appsettings.json`
- Check network connectivity to Fabric endpoints
- Increase command timeout if testing large datasets

### Query Errors
- Verify table name `mxchip_silver` exists in your database
- Ensure column names match the schema
- Check user has SELECT permissions on the table

## Project Structure

```
FabricQueryDemos/
├── Program.cs              # Main orchestrator and runner
├── appsettings.json        # Configuration
├── Models/
│   └── Models.cs           # Data models and results
├── Clients/
│   ├── KqlClient.cs        # KQL endpoint client
│   ├── SqlClient.cs        # SQL endpoint client
│   └── GraphQLClient.cs    # GraphQL endpoint client
└── results/
    ├── report.json         # Detailed JSON results
    └── results.csv         # CSV export of runs
```

## License

MIT