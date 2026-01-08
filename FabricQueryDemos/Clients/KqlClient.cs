using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Data;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;
using Azure.Identity;
using FabricQueryDemos.Models;
using Kusto.Data;
using Kusto.Data.Common;
using Kusto.Data.Net.Client;

namespace FabricQueryDemos.Clients
{
    // ============================================================================
    // Query Client Interface
    // ============================================================================
    // Defines the contract for all query clients. This abstraction allows the
    // test harness to work with different query interfaces (KQL, SQL)
    // without knowing the implementation details.
    // ============================================================================
    
    public interface IQueryClient
    {
        // Executes a query and returns results as dynamic objects (schema-agnostic)
        Task<List<dynamic>> ExecuteQueryAsync(int rowCount);
        
        // Returns the client name for reporting (e.g., "KQL", "SQL")
        string GetClientName();
    }

    // ============================================================================
    // KQL Client for Microsoft Fabric Event Houses
    // ============================================================================
    // Executes Kusto Query Language (KQL) queries against Fabric Event Houses.
    // Uses the official Kusto.Data SDK with Azure AD token-based authentication.
    //
    // Key features:
    // - Dynamic schema support (works with any table structure)
    // - Returns results as ExpandoObjects for flexibility
    // - Uses AzureCliCredential for authentication
    // - Configurable query timeout (3 minutes)
    // ============================================================================
    
    public class KqlClient : IQueryClient
    {
        private readonly string _clusterUri;     // Fabric cluster URI (e.g., https://xyz.kusto.fabric.microsoft.com)
        private readonly string _database;       // Event House database name
        private readonly string _queryTemplate;  // KQL query with {rowCount} placeholder
        private readonly HttpClient _httpClient;

        public KqlClient(string clusterUri, string database, string queryTemplate)
        {
            _clusterUri = clusterUri;
            _database = database;
            _queryTemplate = queryTemplate;
            _httpClient = new HttpClient();
        }

        public string GetClientName() => "KQL";

        public async Task<List<dynamic>> ExecuteQueryAsync(int rowCount)
        {
            try
            {
                // Substitute the row count placeholder in the query template
                // Example: "table | take {rowCount}" becomes "table | take 100"
                var query = _queryTemplate.Replace("{rowCount}", rowCount.ToString());

                // Set up Azure AD authentication for Kusto
                var credential = new AzureCliCredential();
                
                // Token provider function called by Kusto SDK when token is needed
                string AcquireToken()
                {
                    // The scope follows the pattern: {cluster-uri}/.default
                    // This requests access to the specific Fabric cluster
                    var scope = $"{_clusterUri}/.default";
                    var token = credential.GetToken(new Azure.Core.TokenRequestContext(new[] { scope }), default);
                    return token.Token;
                }

                // Build connection string with AAD token authentication
                var kcsb = new KustoConnectionStringBuilder(_clusterUri)
                    .WithAadTokenProviderAuthentication(AcquireToken);

                // Create query provider and configure request properties
                using var queryProvider = KustoClientFactory.CreateCslQueryProvider(kcsb);
                var clientRequestProperties = new ClientRequestProperties();
                clientRequestProperties.SetOption("servertimeout", "00:03:00");  // 3 minute timeout
                clientRequestProperties.ClientRequestId = $"FabricQueryDemos;{Guid.NewGuid()}";  // For tracing/debugging

                // Execute the query and read results
                using var reader = queryProvider.ExecuteQuery(_database, query, clientRequestProperties);
                var rows = ReadRows(reader);
                return rows;
            }
            catch (Exception ex)
            {
                throw new Exception($"KQL query failed: {ex.Message}", ex);
            }
        }

        // Converts IDataReader results to a list of dynamic objects.
        // This approach makes the tool schema-agnostic - it works with any table structure
        // without needing to define a specific class for each table.
        private static List<dynamic> ReadRows(IDataReader reader)
        {
            var results = new List<dynamic>();

            // First pass: discover all column names from the result set
            // This allows us to create dynamic objects with the correct property names
            var columnNames = new List<string>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                columnNames.Add(reader.GetName(i));
            }

            // Second pass: read each row and convert to ExpandoObject
            while (reader.Read())
            {
                // ExpandoObject allows us to add properties dynamically at runtime
                dynamic row = new System.Dynamic.ExpandoObject();
                var rowDict = (IDictionary<string, object>)row;  // Access as dictionary for dynamic property addition

                // Populate each column value as a property on the ExpandoObject
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var value = reader.IsDBNull(i) ? null : reader.GetValue(i);  // Handle NULL values
                    rowDict[columnNames[i]] = value;
                }

                results.Add(row);
            }

            return results;
        }


    }
}
