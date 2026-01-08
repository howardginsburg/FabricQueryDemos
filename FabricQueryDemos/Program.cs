using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Identity;
using CsvHelper;
using Microsoft.Extensions.Configuration;
using FabricQueryDemos.Clients;
using FabricQueryDemos.Models;

// ============================================================================
// Fabric Query Test Engine
// ============================================================================
// A performance testing harness for comparing query performance across
// Microsoft Fabric's query interfaces: KQL (Event Houses) and 
// T-SQL (Lakehouses/Warehouses).
//
// Features:
// - Dynamic schema support (works with any table structure)
// - Multiple iteration sizes for comprehensive testing
// - Statistical analysis (mean, stddev, percentiles)
// - CSV export for further analysis
// - Azure AD authentication via AzureCliCredential
// ============================================================================

class Program
{
    static async Task Main(string[] args)
    {
        try
        {
            // ===== STEP 1: Authentication Validation =====
            // Verify Azure CLI credentials are configured before attempting to connect to Fabric.
            if (!await ValidateAzureCliLogin())
            {
                Console.WriteLine("\nError: Azure CLI is not logged in.");
                Console.WriteLine("Please run 'az login' before running this test harness.");
                Console.WriteLine("\nAlternatively, you can:");
                Console.WriteLine("  - Set environment variables for service principal authentication");
                Console.WriteLine("  - Use managed identity if running on Azure");
                return;
            }

            // ===== STEP 2: Configuration Loading =====
            // Load test parameters and Fabric endpoints from appsettings.json.
            // This includes: cluster URIs, database names, query templates,
            // iteration sizes, and number of runs per iteration.
            var configPath = "appsettings.json";
            if (!File.Exists(configPath))
            {
                Console.WriteLine($"Error: {configPath} not found in {Directory.GetCurrentDirectory()}");
                Console.WriteLine("Please create appsettings.json with your Fabric endpoints.");
                return;
            }

            var config = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile(configPath, optional: false, reloadOnChange: true)
                .Build();

            Console.WriteLine("=== Fabric Query Test Engine ===");

            // ===== STEP 3: Client Initialization =====
            // Create query client instances for each Fabric interface we want to test.
            // Each client encapsulates the connection logic and authentication for its respective API.
            var kqlEndpoint = config["Endpoints:KqlClusterUri"];
            var kqlDatabase = config["Endpoints:KqlDatabase"];
            var sqlEndpoint = config["Endpoints:SqlServerEndpoint"];
            var sqlDatabase = config["Endpoints:SqlDatabase"];
            var graphqlEndpoint = config["Endpoints:GraphQLEndpoint"];

            var kqlQuery = config["TestConfiguration:Queries:Kql"];
            var sqlQuery = config["TestConfiguration:Queries:Sql"];
            var graphqlQuery = config["TestConfiguration:Queries:GraphQL"];

            var clients = new List<IQueryClient>
            {
                new KqlClient(kqlEndpoint, kqlDatabase, kqlQuery),
                new SqlClient(sqlEndpoint, sqlDatabase, sqlQuery),
                //new GraphQLClient(graphqlEndpoint, graphqlQuery)
            };

            var iterationSizes = config.GetSection("TestConfiguration:IterationSizes")
                .Get<int[]>() ?? new[] { 10, 100, 1000, 10000 };
            var runsPerIteration = int.Parse(config["TestConfiguration:RunsPerIteration"] ?? "5");

            var testResult = new TestResult
            {
                StartTime = DateTime.UtcNow
            };

            // ===== STEP 4: Test Execution =====
            // Run performance tests in a nested loop structure:
            // - Outer loop: Different row counts (e.g., 10, 100, 1000, 10000)
            // - Middle loop: Different query clients (KQL, SQL)
            // - Inner loop: Multiple runs for statistical significance (typically 5 runs)
            //
            // This structure allows us to compare performance across both
            // different data sizes and different query interfaces.
            foreach (var size in iterationSizes)
            {
                Console.WriteLine($"\n--- Testing with {size} rows ---");
                
                foreach (var client in clients)
                {
                    Console.WriteLine($"  Running {client.GetClientName()}...");
                    
                    // Execute multiple runs for each combination to get statistical significance
                    for (int run = 1; run <= runsPerIteration; run++)
                    {
                        var sw = Stopwatch.StartNew();
                        try
                        {
                            // Execute the query and measure elapsed time
                            var rows = await client.ExecuteQueryAsync(size);
                            sw.Stop();

                            var queryRun = new QueryRun
                            {
                                IterationSize = size,
                                RunNumber = run,
                                Client = client.GetClientName(),
                                ElapsedMilliseconds = sw.ElapsedMilliseconds,
                                RowCount = rows.Count,
                                PayloadBytes = EstimatePayloadSize(rows.Count),
                                ExecutedAt = DateTime.UtcNow
                            };

                            testResult.Runs.Add(queryRun);
                            Console.WriteLine($"    Run {run}: {sw.ElapsedMilliseconds}ms, {rows.Count} rows");
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"    Run {run}: FAILED - {ex.Message}");
                            testResult.ErrorMessage = (testResult.ErrorMessage ?? "") + $"{client.GetClientName()} run {run}: {ex.Message}\n";
                        }
                    }
                }
            }

            testResult.EndTime = DateTime.UtcNow;
            testResult.Success = string.IsNullOrEmpty(testResult.ErrorMessage);

            // ===== STEP 5: Statistical Analysis =====
            // Calculate aggregated statistics from all runs including:
            // - Mean and standard deviation for latency distribution
            // - Percentiles (P50/median, P95, P99) for tail latency analysis
            // - Throughput in rows per second
            CalculateStatistics(testResult);

            // ===== STEP 6: Results Output =====
            // Generate multiple output formats for analysis:
            // 1. Console report: Human-readable summary
            // 2. Runs CSV: Raw data for each individual query execution
            // 3. Statistics CSV: Aggregated metrics for visualization/comparison
            OutputConsoleReport(testResult);
            await OutputCsvReport(testResult, config);
            await OutputStatisticsCsvReport(testResult, config);

            Console.WriteLine("\n=== Test Completed ===");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Fatal error: {ex}");
            Environment.Exit(1);
        }
    }

    static async Task<bool> ValidateAzureCliLogin()
    {
        try
        {
            // Use Azure CLI identity explicitly to avoid unexpected credential sources
            var credential = new AzureCliCredential();
            
            // Attempt to acquire a token for Azure Management API to verify authentication.
            // This is a lightweight check that doesn't require Fabric-specific permissions.
            // If this fails, none of the Fabric APIs will work either.
            var token = await credential.GetTokenAsync(new Azure.Core.TokenRequestContext(new[] { "https://management.azure.com/.default" }));
            
            // Parse the JWT token to extract and display the authenticated identity.
            // This helps users confirm which account/principal they're using.
            var userInfo = ParseTokenClaims(token.Token);
            
            Console.WriteLine("✓ Azure authentication verified");
            Console.WriteLine($"  Identity: {userInfo}\n");
            return true;
        }
        catch (Azure.Identity.AuthenticationFailedException ex)
        {
            Console.WriteLine($"Authentication failed: {ex.Message}");
            return false;
        }
        catch (Exception)
        {
            // If Azure CLI is not installed or other issue, return false
            return false;
        }
    }

    static string ParseTokenClaims(string token)
    {
        try
        {
            // JWT (JSON Web Token) structure: header.payload.signature
            // We only need the payload section which contains the claims
            var parts = token.Split('.');
            if (parts.Length < 2)
                return "Unknown";

            // The payload is base64url-encoded (not standard base64)
            var payload = parts[1];
            
            // Base64 strings must be a multiple of 4 characters. Add padding if needed.
            // JWT tokens use base64url which omits padding, so we add it back.
            var paddedPayload = payload.PadRight(payload.Length + (4 - payload.Length % 4) % 4, '=');
            var jsonBytes = Convert.FromBase64String(paddedPayload);
            var json = System.Text.Encoding.UTF8.GetString(jsonBytes);
            
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            
            // Extract identity claims from the token in order of preference.
            // Different token types (user, service principal, managed identity)
            // have different claim structures.
            var upn = root.TryGetProperty("upn", out var upnProp) ? upnProp.GetString() : null;
            var email = root.TryGetProperty("email", out var emailProp) ? emailProp.GetString() : null;
            var name = root.TryGetProperty("name", out var nameProp) ? nameProp.GetString() : null;
            var appId = root.TryGetProperty("appid", out var appIdProp) ? appIdProp.GetString() : null;
            var oid = root.TryGetProperty("oid", out var oidProp) ? oidProp.GetString() : null;
            
            // Return the most relevant identifier
            if (!string.IsNullOrEmpty(upn))
                return upn;
            if (!string.IsNullOrEmpty(email))
                return email;
            if (!string.IsNullOrEmpty(name))
                return name;
            if (!string.IsNullOrEmpty(appId))
                return $"Service Principal (AppId: {appId})";
            if (!string.IsNullOrEmpty(oid))
                return $"Identity (OID: {oid})";
            
            return "Authenticated (identity type unknown)";
        }
        catch
        {
            return "Authenticated";
        }
    }

    static void CalculateStatistics(TestResult result)
    {
        // Group test runs by iteration size and client type.
        // This allows us to calculate statistics for each combination separately.
        // Example groups: (10 rows, KQL), (10 rows, SQL), (100 rows, KQL), etc.
        var groupedRuns = result.Runs
            .GroupBy(r => new { r.IterationSize, r.Client })
            .ToList();

        foreach (var group in groupedRuns)
        {
            // Sort latencies for percentile calculations.
            // Percentiles require sorted data.
            var latencies = group.Select(r => r.ElapsedMilliseconds).OrderBy(l => l).ToList();
            
            var stats = new IterationStatistics
            {
                IterationSize = group.Key.IterationSize,
                Client = group.Key.Client,
                RawLatencies = latencies.ToList(),
                Mean = (long)latencies.Average(),  // Average latency
                Min = latencies.First(),            // Best case (fastest)
                Max = latencies.Last(),             // Worst case (slowest)
                P50 = GetPercentile(latencies, 50), // Median - typical performance
                P95 = GetPercentile(latencies, 95), // 95% of queries faster than this
                P99 = GetPercentile(latencies, 99), // 99% of queries faster than this
                TotalRowsRetrieved = group.Sum(r => r.RowCount),
                TotalPayloadBytes = group.Sum(r => r.PayloadBytes)
            };

            // Calculate standard deviation - measures consistency/variability of latencies.
            // Low stddev = consistent performance, high stddev = variable performance
            if (latencies.Count > 1)
            {
                var variance = latencies.Average(x => Math.Pow(x - stats.Mean, 2));
                stats.StdDev = Math.Sqrt(variance);
            }

            // Calculate throughput (rows per second)
            var totalTimeSeconds = latencies.Sum() / 1000.0;
            if (totalTimeSeconds > 0)
                stats.ThroughputRowsPerSecond = stats.TotalRowsRetrieved / totalTimeSeconds;

            result.Statistics.Add(stats);
        }
    }

    static long GetPercentile(List<long> sortedValues, int percentile)
    {
        if (sortedValues.Count == 0) return 0;
        
        // Calculate the index position for the given percentile.
        // Example: For P95 with 100 values: (95/100) * 100 = 95th value
        // We use Ceiling to round up, ensuring we don't underestimate the percentile
        var index = (int)Math.Ceiling((percentile / 100.0) * sortedValues.Count) - 1;
        
        // Clamp the index to valid array bounds to handle edge cases
        return sortedValues[Math.Max(0, Math.Min(index, sortedValues.Count - 1))];
    }

    static void OutputConsoleReport(TestResult result)
    {
        Console.WriteLine("\n\n=== SUMMARY REPORT ===\n");

        foreach (var stat in result.Statistics.OrderBy(s => s.IterationSize).ThenBy(s => s.Client))
        {
            Console.WriteLine($"\n{stat.Client} - {stat.IterationSize} rows:");
            Console.WriteLine($"  Raw Latencies (ms): {string.Join(", ", stat.RawLatencies)}");
            Console.WriteLine($"  Mean:  {stat.Mean}ms");
            Console.WriteLine($"  StdDev: {stat.StdDev:F2}");
            Console.WriteLine($"  Min:   {stat.Min}ms");
            Console.WriteLine($"  Max:   {stat.Max}ms");
            Console.WriteLine($"  P50:   {stat.P50}ms");
            Console.WriteLine($"  P95:   {stat.P95}ms");
            Console.WriteLine($"  P99:   {stat.P99}ms");
            Console.WriteLine($"  Throughput: {stat.ThroughputRowsPerSecond:F2} rows/sec");
            Console.WriteLine($"  Total Rows: {stat.TotalRowsRetrieved}");
            Console.WriteLine($"  Total Payload: {stat.TotalPayloadBytes} bytes");
        }
    }

    static async Task OutputCsvReport(TestResult result, IConfiguration config)
    {
        // Export raw run data to CSV for detailed analysis.
        // Each row represents one query execution with all its metrics.
        // Useful for: creating custom visualizations, statistical analysis, or sharing results.
        var outputPath = config["Output:CsvReportPath"] ?? "./results/results.csv";
        Directory.CreateDirectory(Path.GetDirectoryName(outputPath) ?? "./results");

        using var writer = new StreamWriter(outputPath);
        using var csv = new CsvWriter(writer, System.Globalization.CultureInfo.InvariantCulture);
        
        // CsvHelper automatically generates header from QueryRun properties
        csv.WriteHeader<QueryRun>();
        await csv.NextRecordAsync();
        
        // Write each run as a row in the CSV
        foreach (var run in result.Runs)
        {
            csv.WriteRecord(run);
            await csv.NextRecordAsync();
        }

        Console.WriteLine($"CSV report saved to: {outputPath}");
    }

    static async Task OutputStatisticsCsvReport(TestResult result, IConfiguration config)
    {
        // Export aggregated statistics to CSV for high-level comparison.
        // Each row represents the statistical summary for one (size, client) combination.
        // Useful for: creating comparison charts, identifying performance trends.
        var outputPath = config["Output:StatisticsCsvReportPath"] ?? "./results/statistics.csv";
        Directory.CreateDirectory(Path.GetDirectoryName(outputPath) ?? "./results");

        using var writer = new StreamWriter(outputPath);
        using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);

        // Manually define headers since we're not using CsvHelper's automatic mapping
        csv.WriteField("IterationSize");
        csv.WriteField("Client");
        csv.WriteField("MeanMs");
        csv.WriteField("StdDevMs");
        csv.WriteField("MinMs");
        csv.WriteField("MaxMs");
        csv.WriteField("P50Ms");
        csv.WriteField("P95Ms");
        csv.WriteField("P99Ms");
        csv.WriteField("ThroughputRowsPerSecond");
        csv.WriteField("TotalRowsRetrieved");
        csv.WriteField("TotalPayloadBytes");
        csv.WriteField("RawLatenciesMs");
        await csv.NextRecordAsync();

        foreach (var stat in result.Statistics)
        {
            csv.WriteField(stat.IterationSize);
            csv.WriteField(stat.Client);
            csv.WriteField(stat.Mean);
            csv.WriteField(stat.StdDev);
            csv.WriteField(stat.Min);
            csv.WriteField(stat.Max);
            csv.WriteField(stat.P50);
            csv.WriteField(stat.P95);
            csv.WriteField(stat.P99);
            csv.WriteField(stat.ThroughputRowsPerSecond);
            csv.WriteField(stat.TotalRowsRetrieved);
            csv.WriteField(stat.TotalPayloadBytes);
            csv.WriteField(string.Join(";", stat.RawLatencies));
            await csv.NextRecordAsync();
        }

        Console.WriteLine($"Statistics CSV saved to: {outputPath}");
    }

    static long EstimatePayloadSize(int rowCount)
    {
        // Rough estimation: JSON would be larger, estimate average per row
        return rowCount * 250; // Approximate bytes per row
    }
}
