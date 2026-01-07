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

class Program
{
    static async Task Main(string[] args)
    {
        try
        {
            // Validate Azure CLI login
            if (!await ValidateAzureCliLogin())
            {
                Console.WriteLine("\nError: Azure CLI is not logged in.");
                Console.WriteLine("Please run 'az login' before running this test harness.");
                Console.WriteLine("\nAlternatively, you can:");
                Console.WriteLine("  - Set environment variables for service principal authentication");
                Console.WriteLine("  - Use managed identity if running on Azure");
                return;
            }

            // Load configuration
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

            Console.WriteLine("=== Fabric Query Test Engine ===\n");

            // Initialize clients
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

            // Run tests
            foreach (var size in iterationSizes)
            {
                Console.WriteLine($"\n--- Testing with {size} rows ---");
                
                foreach (var client in clients)
                {
                    Console.WriteLine($"  Running {client.GetClientName()}...");
                    
                    for (int run = 1; run <= runsPerIteration; run++)
                    {
                        var sw = Stopwatch.StartNew();
                        try
                        {
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

            // Calculate statistics
            CalculateStatistics(testResult);

            // Output results
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
            var credential = new DefaultAzureCredential();
            // Try to get a token for a common scope to verify authentication
            var token = await credential.GetTokenAsync(new Azure.Core.TokenRequestContext(new[] { "https://management.azure.com/.default" }));
            
            // Parse token to extract user information
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
            // JWT tokens have three parts separated by dots: header.payload.signature
            var parts = token.Split('.');
            if (parts.Length < 2)
                return "Unknown";

            // Decode the payload (second part)
            var payload = parts[1];
            // Add padding if needed for base64 decoding
            var paddedPayload = payload.PadRight(payload.Length + (4 - payload.Length % 4) % 4, '=');
            var jsonBytes = Convert.FromBase64String(paddedPayload);
            var json = System.Text.Encoding.UTF8.GetString(jsonBytes);
            
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            
            // Try to extract useful identity information
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
        var groupedRuns = result.Runs
            .GroupBy(r => new { r.IterationSize, r.Client })
            .ToList();

        foreach (var group in groupedRuns)
        {
            var latencies = group.Select(r => r.ElapsedMilliseconds).OrderBy(l => l).ToList();
            
            var stats = new IterationStatistics
            {
                IterationSize = group.Key.IterationSize,
                Client = group.Key.Client,
                RawLatencies = latencies.ToList(),
                Mean = (long)latencies.Average(),
                Min = latencies.First(),
                Max = latencies.Last(),
                P50 = GetPercentile(latencies, 50),
                P95 = GetPercentile(latencies, 95),
                P99 = GetPercentile(latencies, 99),
                TotalRowsRetrieved = group.Sum(r => r.RowCount),
                TotalPayloadBytes = group.Sum(r => r.PayloadBytes)
            };

            // Calculate standard deviation
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
        
        var index = (int)Math.Ceiling((percentile / 100.0) * sortedValues.Count) - 1;
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
        var outputPath = config["Output:CsvReportPath"] ?? "./results/results.csv";
        Directory.CreateDirectory(Path.GetDirectoryName(outputPath) ?? "./results");

        using var writer = new StreamWriter(outputPath);
        using var csv = new CsvWriter(writer, System.Globalization.CultureInfo.InvariantCulture);
        
        csv.WriteHeader<QueryRun>();
        await csv.NextRecordAsync();
        
        foreach (var run in result.Runs)
        {
            csv.WriteRecord(run);
            await csv.NextRecordAsync();
        }

        Console.WriteLine($"CSV report saved to: {outputPath}");
    }

    static async Task OutputStatisticsCsvReport(TestResult result, IConfiguration config)
    {
        var outputPath = config["Output:StatisticsCsvReportPath"] ?? "./results/statistics.csv";
        Directory.CreateDirectory(Path.GetDirectoryName(outputPath) ?? "./results");

        using var writer = new StreamWriter(outputPath);
        using var csv = new CsvWriter(writer, CultureInfo.InvariantCulture);

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
