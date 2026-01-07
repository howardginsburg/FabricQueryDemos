using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Azure.Identity;
using FabricQueryDemos.Models;
using Microsoft.Data.SqlClient;

namespace FabricQueryDemos.Clients
{
    public class SqlClient : IQueryClient
    {
        private readonly string _serverEndpoint;
        private readonly string _database;
        private readonly string _queryTemplate;

        public SqlClient(string serverEndpoint, string database, string queryTemplate)
        {
            _serverEndpoint = serverEndpoint;
            _database = database;
            _queryTemplate = queryTemplate;
        }

        public string GetClientName() => "SQL";

        public async Task<List<dynamic>> ExecuteQueryAsync(int rowCount)
        {
            // Simple connection string - let Azure SDK handle authentication
            var connectionString = new SqlConnectionStringBuilder
            {
                DataSource = _serverEndpoint,
                InitialCatalog = _database,
                Encrypt = true,
                TrustServerCertificate = false
            }.ConnectionString;

            using var connection = new SqlConnection(connectionString);
            
            // Use DefaultAzureCredential - automatically uses az login, environment variables, managed identity, etc.
            var credential = new DefaultAzureCredential();
            var token = await credential.GetTokenAsync(
                new Azure.Core.TokenRequestContext(new[] { "https://database.windows.net/.default" }));
            
            connection.AccessToken = token.Token;
            
            await connection.OpenAsync();
            //Console.WriteLine($"    Connected to {_database}");

            // Execute query
            var query = _queryTemplate.Replace("@rowCount", rowCount.ToString());
            using var command = new SqlCommand(query, connection)
            {
                CommandTimeout = 300
            };

            var rows = new List<dynamic>();
            using var reader = await command.ExecuteReaderAsync();
            
            while (await reader.ReadAsync())
            {
                dynamic row = new System.Dynamic.ExpandoObject();
                var rowDict = (IDictionary<string, object>)row;

                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                    rowDict[reader.GetName(i)] = value;
                }

                rows.Add(row);
            }

            return rows;
        }
    }
}
