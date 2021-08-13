using Microsoft.Identity.Client;
using Microsoft.PowerBI.Api;
using Microsoft.Rest;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.PowerBI.Api.Models;
using TabModel = Microsoft.AnalysisServices.Tabular;
using Microsoft.AnalysisServices.AdomdClient;
using System.Text.RegularExpressions;
using System.Security;
using System.Threading;

namespace Sqlbi.PbiPushDataset
{
    public class PbiPushDatasetException : Exception
    {
        public PbiConnection Connection { get; private set; }
        public PbiPushDatasetException( PbiConnection connection, string message ) : base( message ) {
            Connection = connection;
        }
    }

    public class PbiConnection
    {
        // Power BI API settings
        private const string resource = "https://analysis.windows.net/powerbi/api";
        private const string ApiUrl = "https://api.powerbi.com";

        public string TenantId { get; set; }
        public string PrincipalId { get; set; }
        public string ClientSecret { get; set; }

        // Properties for user authentication (required for Refresh in PbiPushDataset)
        public string ClientId { get; set; }
        public string Username { get; set; }
        public string Password { get; set; }

        public PbiConnection()
        {
        }

        private PowerBIClient _powerBIClient;
        public bool IsOpen { get => _powerBIClient != null; }

        public async Task<string> GetServicePrincipalToken()
        {
            IConfidentialClientApplication appBuilder = null;

            if (appBuilder == null)
            {
                appBuilder = ConfidentialClientApplicationBuilder
                    .Create(PrincipalId)
                    .WithClientSecret(ClientSecret)
                    .WithAuthority(new Uri($"https://login.microsoftonline.com/{TenantId}"))
                    .Build();
            }

            AuthenticationResult result;
            try
            {
                // Use .default to get all the permissions available (those configured in AAD for this app)
                string[] scopes = new string[] { $"{resource}/.default" };

                result = await appBuilder.AcquireTokenForClient(scopes).ExecuteAsync();
            }
            catch (MsalUiRequiredException)
            {
                // The application doesn't have sufficient permissions.
                // - Did you declare enough app permissions during app creation?
                // - Did the tenant admin grant permissions to the application?
                
                throw;
            }
            catch (MsalServiceException ex) when (ex.Message.Contains("AADSTS70011"))
            {
                // Invalid scope. The scope has to be in the form "https://resourceurl/.default"
                // Mitigation: Change the scope to be as expected.
                
                throw;
            }

            return result.AccessToken;
        }

        public async Task<string> GetUserAccessToken(string serverResource = null)
        {
            IPublicClientApplication appBuilder = null;

            if (appBuilder == null)
            {
                appBuilder = PublicClientApplicationBuilder
                    .Create(ClientId)
                    .WithTenantId(TenantId)
                    .Build();
            }

            // Use AAS or PBI depending on server type
            string resource = serverResource ?? @"https://analysis.windows.net/powerbi/api";

            // TODO: check whether we can ask for read only permissions
            string[] scopes = new string[] { $"{resource}/.default" };

            var accounts = await appBuilder.GetAccountsAsync();
            AuthenticationResult result = null;
            try
            {
                var account = accounts.FirstOrDefault();
                if (account != null)
                {
                    result = await appBuilder.AcquireTokenSilent(scopes, account).ExecuteAsync();
                }
            }
            catch (MsalUiRequiredException)
            {
                // Ignore exception, result is still null and requires authentication
            }

            if (result == null)
            {
                try
                {
                    if (!string.IsNullOrWhiteSpace(Username) && !string.IsNullOrWhiteSpace(Password))
                    {
                        var securePassword = new SecureString();
                        foreach (char c in Password)        // you should fetch the password
                            securePassword.AppendChar(c);  // keystroke by keystroke

                        result = await appBuilder.AcquireTokenByUsernamePassword(scopes, Username, securePassword).ExecuteAsync();
                    }
                }
                catch (Exception)
                {
                    // TODO log error 
                    throw;
                }
            }
            return result.AccessToken;
        }

        public async Task Open()
        {
            if (IsOpen) return;

            #region CreateAccessToken
            string authority = $"https://login.microsoftonline.com/{TenantId}";
            IConfidentialClientApplication app = ConfidentialClientApplicationBuilder
                    .Create(PrincipalId)
                    .WithClientSecret(ClientSecret)
                    .WithAuthority(new Uri(authority))
                    .Build();

            // Use .default to get all the permissions available (those configured in AAD for this app)
            string[] scopes = new string[] { $"{resource}/.default" };

            AuthenticationResult result;
            try
            {
                result = await app.AcquireTokenForClient(scopes).ExecuteAsync();
            }
            catch (MsalUiRequiredException )
            {
                // The application doesn't have sufficient permissions.
                // - Did you declare enough app permissions during app creation?
                // - Did the tenant admin grant permissions to the application?
                throw; 
            }
            catch (MsalServiceException ex) when (ex.Message.Contains("AADSTS70011"))
            {
                // Invalid scope. The scope has to be in the form "https://resourceurl/.default"
                // Mitigation: Change the scope to be as expected.
                throw; 
            }

            #endregion

            var tokenCredentials = new TokenCredentials(result.AccessToken, "Bearer");
            _powerBIClient = new PowerBIClient(new Uri(ApiUrl), tokenCredentials);
        }

        public async Task<string> CreatePushDataset( 
            FileInfo model, 
            Guid groupId, 
            string datasetName, 
            bool overwriteExistingDataset, 
            bool fifoPolicy,
            Action<TabModel.Measure> unsupportedMeasureAction,
            Action<TabModel.Relationship> unsupportedRelationshipAction)
        {
            await Open();

            var datasets_async = await _powerBIClient.Datasets.GetDatasetsInGroupAsync(groupId);
            var datasets = datasets_async.Value;
            var ds = datasets.Where(d => d.Name == datasetName).FirstOrDefault();
            if (ds != null)
            {
                if (overwriteExistingDataset)
                {
                    await _powerBIClient.Datasets.DeleteDatasetAsync(ds.Id);
                }
                else
                {
                    throw new PbiPushDatasetException( this, $"Dataset {datasetName} already existing." );
                }
            }

            string modelBim = File.ReadAllText(model.FullName);
            TabModel.Database database = TabModel.JsonSerializer.DeserializeDatabase(modelBim);
            var schema = SchemaBuilder.GetDatasetRequest( datasetName, database.Model, unsupportedMeasureAction, unsupportedRelationshipAction);

            // Version for single user
            // ds = api.Datasets.PostDataset(schema, DefaultRetentionPolicy.BasicFIFO);
            ds = await _powerBIClient.Datasets.PostDatasetInGroupAsync(
                groupId, 
                schema, 
                fifoPolicy ? DefaultRetentionPolicy.BasicFIFO : DefaultRetentionPolicy.None
            );

            return ds.Id;
        }

        public async Task<List<string>> AlterPushDataset(
            FileInfo model,
            Guid groupId,
            string datasetName,
            Action<TabModel.Measure> unsupportedMeasureAction,
            Action<TabModel.Relationship> unsupportedRelationshipAction)
        {
            await Open();

            var datasets = await _powerBIClient.Datasets.GetDatasetsInGroupAsync(groupId);
            var ds = datasets.Value.Where(d => d.Name == datasetName).FirstOrDefault();
            if (ds == null)
            {
                throw new PbiPushDatasetException(this, $"Dataset {datasetName} not found.");
            }
            return await AlterPushDataset(model, groupId, new Guid(ds.Id), unsupportedMeasureAction, unsupportedRelationshipAction);
        }

        public async Task<List<string>> AlterPushDataset(
            FileInfo model,
            Guid groupId,
            Guid datasetId,
            Action<TabModel.Measure> unsupportedMeasureAction,
            Action<TabModel.Relationship> unsupportedRelationshipAction)
        {
            await Open();

            string modelBim = File.ReadAllText(model.FullName);
            TabModel.Database database = TabModel.JsonSerializer.DeserializeDatabase(modelBim);
            var schema = SchemaBuilder.GetDatasetRequest(datasetId.ToString(), database.Model, unsupportedMeasureAction, unsupportedRelationshipAction);

            List<string> updatedTables = new List<string>();
            // Execute an update for each table
            foreach (Table table in schema.Tables)
            {
                var updatedTable = await _powerBIClient.Datasets.PutTableInGroupAsync(
                    groupId,
                    datasetId.ToString(),
                    table.Name,
                    table
                    );
                updatedTables.Add(updatedTable.Name);
            }
            return updatedTables;
        }

        public async Task<List<string>> ClearPushDataset(Guid groupId, string datasetName, Action<string> clearingTable = null)
        {
            await Open();

            var datasets = await _powerBIClient.Datasets.GetDatasetsInGroupAsync(groupId);
            var ds = datasets.Value.Where(d => d.Name == datasetName).FirstOrDefault();
            if (ds == null)
            {
                throw new PbiPushDatasetException(this, $"Dataset {datasetName} not found.");
            }
            return await ClearPushDataset(groupId, new Guid(ds.Id), clearingTable);
        }

        public async Task<List<string>> ClearPushDataset( Guid groupId, Guid datasetId, Action<string> clearingTable = null)
        {
            await Open();
            var clearedTables = new List<string>();
            var tables = await _powerBIClient.Datasets.GetTablesInGroupAsync(groupId, datasetId.ToString());
            foreach( var t in tables.Value )
            {
                clearingTable?.Invoke(t.Name);
                await _powerBIClient.Datasets.DeleteRowsInGroupAsync(groupId, datasetId.ToString(), t.Name);
                clearedTables?.Add(t.Name);
            }
            return clearedTables;
        }

        public async Task<List<(string, int)>> PushSimulation(Guid groupId, string datasetName, Simulator simulator)
        {
            await Open();

            var datasets = await _powerBIClient.Datasets.GetDatasetsInGroupAsync(groupId);
            var ds = datasets.Value.Where(d => d.Name == datasetName).FirstOrDefault();
            if (ds == null)
            {
                throw new PbiPushDatasetException(this, $"Dataset {datasetName} not found.");
            }
            return await PushSimulation(groupId, new Guid(ds.Id), simulator);
        }

        public async Task<List<(string, int)>> PushSimulation(Guid groupId, Guid datasetId, Simulator simulator)
        {
            await Open();

            var pushedTables = new List<(string, int)>();
            foreach ( var table in simulator.Parameters.Tables )
            {
                List<object> rows = new List<object>();
                for (int rowNumber = 0; rowNumber < table.BatchRows; rowNumber++)
                {
                    rows.Add(table.GenerateRow());
                }
                await _powerBIClient.Datasets.PostRowsInGroupAsync(groupId, datasetId.ToString(), table.Name, new PostRowsRequest(rows));
                pushedTables.Add((table.Name, rows.Count));
            }

            return pushedTables;
        }

        public async Task<List<(string,int)>> RefreshWithDax(
            Guid groupId, string datasetName, 
            string sourceWorkspace, string sourceDatabase, 
            string dax, 
            Action<string, int> refreshingTable = null, 
            bool clearTable = true)
        {
            await Open();

            var datasets = await _powerBIClient.Datasets.GetDatasetsInGroupAsync(groupId);
            var ds = datasets.Value.Where(d => d.Name == datasetName).FirstOrDefault();
            if (ds == null)
            {
                throw new PbiPushDatasetException(this, $"Dataset {datasetName} not found.");
            }
            return await RefreshWithDax(groupId, new Guid(ds.Id), sourceWorkspace, sourceDatabase, dax, refreshingTable, clearTable);
        }

        public async Task<List<(string, int)>> RefreshWithDax(
            Guid groupId, Guid datasetId,
            string sourceWorkspace, string sourceDatabase,
            string dax, 
            Action<string, int> refreshingTable = null, 
            bool clearTable = true)
        {
            string connectionString = ConnectionHelper.GetOleDbPBIConnectionString(sourceWorkspace, sourceDatabase);
            return await RefreshWithQuery(groupId, datasetId, connectionString, dax, refreshingTable, clearTable);
        }

        public async Task<List<(string, int)>> RefreshWithQuery(Guid groupId, Guid datasetId, string connectionString, string query, Action<string, int> refreshingTable = null, bool clearTable = true)
        {
            const int MAX_ROWS_PER_POST = 9000;

            await Open();

            if (!string.IsNullOrWhiteSpace(Username))
            {
                // TODO implement support for AAS as a server source instead of PBI Service
                //       pass "https://northcentralus.asazure.windows.net/" to GetUserAccessToken using the proper region to locate the AAS server;
                //       passing NULL or no params to GetUserAccessToken we use https://analysis.windows.net/powerbi/api as a default

                // Add user credential to connectionString
                connectionString += $";Password={await GetUserAccessToken()}";
            }
            else
            {
                // Add token from service principal account
                connectionString += $";Password={await GetServicePrincipalToken()}";
            }

            var refreshedTables = new List<(string, int)>();
            using AdomdConnection connection = new AdomdConnection(connectionString);
            connection.Open();
            using AdomdCommand cmd = connection.CreateCommand();
            cmd.CommandText = query;
            using var reader = cmd.ExecuteReader();
            do
            {
                List<object> rows = new List<object>();
                string tableName = string.Empty;
                string[] fieldNames = new string[reader.FieldCount];
                int totalRows = 0;
                // Prepare table and field names
                for (int col = 0; col < reader.FieldCount; col++)
                {
                    string columnName = reader.GetName(col);
                    // TODO - fix the RegEx to get the right table name and remove ' at the boundaries
                    string fieldTable = Regex.Match(columnName, @"[^\[]*").Value;
                    if (!string.IsNullOrEmpty(fieldTable))
                    {
                        if (fieldTable[0] == '\'')
                        {
                            fieldTable = fieldTable[1..^1];
                        }
                    }
                    if (string.IsNullOrEmpty(tableName))
                    {
                        // TODO: remove workaround if/when Date name bug is fixed by Microsoft
                        tableName = (fieldTable == "Date") ? "Dates" : fieldTable;
                    }
                    // Ignore fields that are not part of the table
                    if (fieldTable == tableName || string.IsNullOrEmpty(fieldTable) || (fieldTable == "Date" && tableName == "Dates"))
                    {
                        fieldNames[col] = Regex.Match(columnName, @"(?<=\[).+?(?=\])").Value;
                    }
                }

                refreshingTable?.Invoke(tableName, -1);

                // Clear table if required
                if (clearTable)
                {
                    await _powerBIClient.Datasets.DeleteRowsInGroupAsync(groupId, datasetId.ToString(), tableName);
                }

                while (reader.Read())
                {
                    var rowPushData = new System.Dynamic.ExpandoObject();
                    for (int col = 0; col < reader.FieldCount; col++)
                    {
                        if (!string.IsNullOrEmpty(fieldNames[col]))
                        {
                            object value = reader.GetValue(col);

                            rowPushData.TryAdd(fieldNames[col], value);
                        }
                    }
                    rows.Add(rowPushData);

                    if (rows.Count >= MAX_ROWS_PER_POST)
                    {
                        refreshingTable?.Invoke(tableName, rows.Count);
                        totalRows += rows.Count;
                        await _powerBIClient.Datasets.PostRowsInGroupAsync(groupId, datasetId.ToString(), tableName, new PostRowsRequest(rows));
                        rows = new List<object>();
                    }

                }
                refreshingTable?.Invoke(tableName, rows.Count);
                totalRows += rows.Count;
                await _powerBIClient.Datasets.PostRowsInGroupAsync(groupId, datasetId.ToString(), tableName, new PostRowsRequest(rows));
                refreshedTables.Add((tableName, totalRows));
            } while (reader.NextResult());

            return refreshedTables;
        }

    }
}
