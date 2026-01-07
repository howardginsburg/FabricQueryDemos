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
    public interface IQueryClient
    {
        Task<List<dynamic>> ExecuteQueryAsync(int rowCount);
        string GetClientName();
    }

    public class KqlClient : IQueryClient
    {
        private readonly string _clusterUri;
        private readonly string _database;
        private readonly string _queryTemplate;
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
                var query = _queryTemplate.Replace("{rowCount}", rowCount.ToString());

                // Use DefaultAzureCredential for automatic credential discovery
                var credential = new DefaultAzureCredential();
                string AcquireToken()
                {
                    var scope = $"{_clusterUri}/.default";
                    var token = credential.GetToken(new Azure.Core.TokenRequestContext(new[] { scope }), default);
                    return token.Token;
                }

                var kcsb = new KustoConnectionStringBuilder(_clusterUri)
                    .WithAadTokenProviderAuthentication(AcquireToken);

                using var queryProvider = KustoClientFactory.CreateCslQueryProvider(kcsb);
                var clientRequestProperties = new ClientRequestProperties();
                clientRequestProperties.SetOption("servertimeout", "00:03:00");
                clientRequestProperties.ClientRequestId = $"FabricQueryDemos;{Guid.NewGuid()}";

                using var reader = queryProvider.ExecuteQuery(_database, query, clientRequestProperties);
                var rows = ReadRows(reader);
                return rows;
            }
            catch (Exception ex)
            {
                throw new Exception($"KQL query failed: {ex.Message}", ex);
            }
        }

        private static List<dynamic> ReadRows(IDataReader reader)
        {
            var results = new List<dynamic>();

            // Get all column names dynamically
            var columnNames = new List<string>();
            for (int i = 0; i < reader.FieldCount; i++)
            {
                columnNames.Add(reader.GetName(i));
            }

            while (reader.Read())
            {
                dynamic row = new System.Dynamic.ExpandoObject();
                var rowDict = (IDictionary<string, object>)row;

                for (int i = 0; i < reader.FieldCount; i++)
                {
                    var value = reader.IsDBNull(i) ? null : reader.GetValue(i);
                    rowDict[columnNames[i]] = value;
                }

                results.Add(row);
            }

            return results;
        }


    }
}
