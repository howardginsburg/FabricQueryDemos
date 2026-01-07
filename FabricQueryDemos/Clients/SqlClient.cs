using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Identity;
using FabricQueryDemos.Models;
using Microsoft.Data.SqlClient;

namespace FabricQueryDemos.Clients
{
    // ============================================================================
    // SQL Client for Microsoft Fabric Lakehouses/Warehouses
    // ============================================================================
    // Executes T-SQL queries against Fabric SQL endpoints (Lakehouse or Warehouse).
    // Uses Microsoft.Data.SqlClient with Azure AD access token authentication.
    //
    // Key features:
    // - Dynamic schema support (works with any table structure)
    // - Returns results as ExpandoObjects for flexibility
    // - Token-based authentication (no password needed)
    // - Encrypted connections by default
    // ============================================================================
    
    public class SqlClient : IQueryClient
    {
        private readonly string _serverEndpoint;  // SQL endpoint (e.g., xyz.datawarehouse.fabric.microsoft.com)
        private readonly string _database;        // Database name
        private readonly string _queryTemplate;   // T-SQL query with @rowCount placeholder

        public SqlClient(string serverEndpoint, string database, string queryTemplate)
        {
            _serverEndpoint = serverEndpoint;
            _database = database;
            _queryTemplate = queryTemplate;
        }

        public string GetClientName() => "SQL";

        public async Task<List<dynamic>> ExecuteQueryAsync(int rowCount)
        {
            // Build SQL connection string without username/password.
            // Authentication will be handled via access token below.
            var connectionString = new SqlConnectionStringBuilder
            {
                DataSource = _serverEndpoint,          // SQL endpoint hostname
                InitialCatalog = _database,            // Database to connect to
                Encrypt = true,                        // Required for Azure SQL/Fabric
                TrustServerCertificate = false         // Validate server certificate
            }.ConnectionString;

            using var connection = new SqlConnection(connectionString);
            
            // Acquire an Azure AD access token for SQL Database.
            // The scope "https://database.windows.net/.default" works for both
            // Azure SQL Database and Microsoft Fabric SQL endpoints.
            var credential = new DefaultAzureCredential();
            var token = await credential.GetTokenAsync(
                new Azure.Core.TokenRequestContext(new[] { "https://database.windows.net/.default" }));
            
            // Set the access token on the connection (alternative to username/password)
            connection.AccessToken = token.Token;
            
            await connection.OpenAsync();

            // Substitute the row count parameter in the query.
            // Example: "SELECT TOP (@rowCount) * FROM table" becomes "SELECT TOP (100) * FROM table"
            var query = _queryTemplate.Replace("@rowCount", rowCount.ToString());
            using var command = new SqlCommand(query, connection)
            {
                CommandTimeout = 300  // 5 minute timeout for large queries
            };

            // Execute query and convert results to dynamic objects
            var rows = new List<dynamic>();
            using var reader = await command.ExecuteReaderAsync();
            
            // Read each row and build an ExpandoObject with column values
            while (await reader.ReadAsync())
            {
                dynamic row = new System.Dynamic.ExpandoObject();
                var rowDict = (IDictionary<string, object>)row;

                // Add each column as a dynamic property
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var value = reader.IsDBNull(i) ? null : reader.GetValue(i);  // Handle NULL
                    rowDict[reader.GetName(i)] = value;  // Use column name as property name
                }

                rows.Add(row);
            }

            return rows;
        }
    }
}
