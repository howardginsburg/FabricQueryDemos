# Fabric Query Test Engine

A .NET console application for benchmarking and comparing query execution across Microsoft Fabric endpoints: **KQL (Event Houses)** and **SQL (Lakehouses/Warehouses)**. The engine is fully **schema-agnostic** and works with any table structure using dynamic types.

## Features

- **Schema-Agnostic Design**: Works with any table structure without requiring predefined models
- **Multi-Endpoint Testing**: Execute queries against KQL and SQL endpoints simultaneously
- **Flexible Configuration**: Customizable row counts and iterations to measure scaling behavior
- **Statistical Analysis**: Calculate mean, standard deviation, min, max, and percentiles (P50, P95, P99)
- **Multiple Runs**: Execute each test iteration multiple times to ensure statistical reliability
- **Raw & Aggregate Metrics**: Report all raw latencies plus computed statistics with throughput
- **Azure AD Authentication**: Seamless authentication using AzureCliCredential (user or service principal via `az login`)
- **Multi-Format Output**: Console report and dual CSV exports (individual runs + aggregated statistics)
- **Well-Documented Code**: Comprehensive inline comments explaining logic and design decisions

## Prerequisites

- .NET 9.0 or later
- Access to Microsoft Fabric Event House and/or Lakehouse/Warehouse
- Azure authentication via Azure CLI (`az login` for users, `az login --service-principal` for app identities)

## Configuration

Create or edit `appsettings.json` to configure your Fabric endpoints and test parameters. A `sampleappsettings.json` template is provided for reference.

```json
{
  "Endpoints": {
    "KqlClusterUri": "https://YOUR_CLUSTER.kusto.fabric.microsoft.com",
    "KqlDatabase": "YOUR_DATABASE_NAME",
    "SqlServerEndpoint": "YOUR_LAKEHOUSE_ENDPOINT.datawarehouse.fabric.microsoft.com",
    "SqlDatabase": "YOUR_DATABASE_NAME"
  },
  "TestConfiguration": {
    "IterationSizes": [ 10, 100, 1000, 10000, 100000 ],
    "RunsPerIteration": 5,
    "Queries": {
      "Kql": "YOUR_TABLE | order by YOUR_TIMESTAMP_FIELD asc | take {rowCount}",
      "Sql": "SELECT TOP (@rowCount) * FROM dbo.YOUR_TABLE ORDER BY YOUR_TIMESTAMP_FIELD"
    }
  },
  "Output": {
    "ConsoleReport": true,
    "CsvReportPath": "./results/results.csv",
    "StatisticsCsvReportPath": "./results/statistics.csv"
  }
}
```

### Getting Your Endpoints

1. **KQL Cluster URI**: Available in your Fabric Event House settings
2. **SQL Server Endpoint**: Found in your Lakehouse or Warehouse SQL connection settings

### Query Templates

The tool uses query templates with placeholders:
- **KQL**: Use `{rowCount}` placeholder (e.g., `table | take {rowCount}`)
- **SQL**: Use `@rowCount` placeholder (e.g., `SELECT TOP (@rowCount) * FROM table`)

## Running the Application

```bash
# Authenticate with Azure (if not already authenticated)
az login

# Navigate to the project directory
cd FabricQueryDemos

# Build the application
dotnet build

# Run the tests
dotnet run
```

The application will automatically use your Azure CLI credentials via AzureCliCredential.

## Output

The application generates three types of output:

### 1. Console Output
Real-time progress and summary statistics displayed in the terminal:
```
=== Fabric Query Test Engine ===
Authenticated as: user@domain.com

--- Testing with 10 rows ---
  Running KQL...
    Run 1: 234ms, 10 rows
    Run 2: 245ms, 10 rows
    ...
  Running SQL...
    Run 1: 156ms, 10 rows
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
  Throughput: 41.84 rows/sec
  Total Rows: 50
  Total Payload: 12500 bytes
```

### 2. Results CSV (`results/results.csv`)
Individual run data with one row per query execution. Useful for detailed analysis and visualization:
```csv
IterationSize,RunNumber,Client,ElapsedMilliseconds,RowCount,PayloadBytes,ExecutedAt
10,1,KQL,234,10,2500,2026-01-07T10:30:45Z
10,2,KQL,245,10,2500,2026-01-07T10:30:46Z
...
```

### 3. Statistics CSV (`results/statistics.csv`)
Aggregated statistics with one row per (size, client) combination. Ideal for comparison charts:
```csv
IterationSize,Client,Mean,StdDev,Min,Max,P50,P95,P99,ThroughputRowsPerSecond,TotalRowsRetrieved,TotalPayloadBytes,RawLatencies
10,KQL,239,3.74,234,245,239,245,245,41.84,50,12500,"234,245,238,241,239"
10,SQL,156,2.45,153,159,156,159,159,64.10,50,12500,"153,157,156,155,159"
...
```

## Understanding the Metrics

- **Raw Latencies**: All individual execution times in milliseconds (configurable via RunsPerIteration)
- **Mean**: Average latency across all runs
- **StdDev**: Standard deviation indicating performance consistency (lower = more consistent)
- **Min/Max**: Best and worst-case latencies
- **P50 (Median)**: 50th percentile - typical performance
- **P95**: 95th percentile - performance SLA target (95% of queries faster than this)
- **P99**: 99th percentile - tail latency (99% of queries faster than this)
- **Throughput**: Average rows retrieved per second (calculated as: rows × runs / total_time)
- **Payload Bytes**: Estimated size of data transferred (all runs combined)

## Schema-Agnostic Design

The tool uses **dynamic types** (`ExpandoObject`) to work with any table structure without requiring predefined classes:

- **No hardcoded schemas**: Works with any table, any columns
- **Dynamic discovery**: Automatically reads column names and types at runtime
- **Flexible**: Easily test different tables by just changing the query template
- **Type-safe internally**: Proper type mapping from data sources to .NET types

This makes the tool perfect for:
- Testing different tables without code changes
- Working with evolving schemas
- Comparing performance across different table structures
- Quick ad-hoc performance testing

## Troubleshooting

### Authentication Issues
- Run `az login` and authenticate with your Microsoft account
- Ensure your account has access to the Fabric workspace
- Check that workspace permissions include read access to Event Houses/Lakehouses
- For service principal auth, set environment variables: `AZURE_CLIENT_ID`, `AZURE_TENANT_ID`, `AZURE_CLIENT_SECRET`

### Connection Timeouts
- Verify endpoints are correct in `appsettings.json`
- Ensure cluster URIs use the full domain (e.g., `.kusto.fabric.microsoft.com`)
- Check network connectivity to Fabric endpoints
- KQL timeout: 3 minutes (configurable in KqlClient)
- SQL timeout: 5 minutes (configurable in SqlClient)

### Query Errors
- Verify table exists in your database
- Check query template syntax for your data source
- Ensure user has SELECT permissions on the target table
- Test queries manually in Fabric portal before running the tool

## Project Structure

```
FabricQueryDemos/
├── Program.cs                    # Main orchestrator, statistics, and output
├── appsettings.json              # Configuration (endpoints, queries, output paths)
├── sampleappsettings.json        # Configuration template
├── Models/
│   └── Models.cs                 # Data models (QueryRun, IterationStatistics, TestResult)
├── Clients/
│   ├── KqlClient.cs              # KQL Event House client with dynamic row handling
│   └── SqlClient.cs              # SQL Lakehouse/Warehouse client with AAD token auth
└── results/
    ├── results.csv               # Individual query execution data
    └── statistics.csv            # Aggregated performance statistics
```

## Key Implementation Details

- **Interface-based abstraction**: `IQueryClient` interface allows easy addition of new query clients
- **AzureCliCredential**: Uses the signed-in Azure CLI identity for token acquisition
- **Dynamic row construction**: `ExpandoObject` for schema-agnostic data handling
- **Statistical rigor**: Multiple runs with percentile calculations for reliable metrics
- **Comprehensive logging**: Inline comments throughout codebase explain logic and decisions

## License

MIT
