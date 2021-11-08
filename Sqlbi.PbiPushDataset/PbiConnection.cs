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

/*
 * TODO
 * 
 * In order to implement the RefreshWithDax using the Power BI REST API,
 * we have to implement the following:
 *   - Provide a folder path with a list of files, not just a single DAX file
 *   - Each file must have a single DAX query
 *   - When the DAX query contains the parameter @Rows and @Skip, replace them 
 *     with the number of rows to include (less than or equal to 100,000) and 
 *     the number of rows to skip. The sort order must be defined by the query author
 *     If a query doesn't have a TOPNSKIP function, it cannot process more than 100,000 rows
 * 
 */
namespace Sqlbi.PbiPushDataset
{
    public class PbiPushDatasetException : Exception
    {
        public PbiConnection Connection { get; private set; }
        public PbiPushDatasetException( PbiConnection connection, string message ) : base( message ) {
            Connection = connection;
        }
    }

    /// <summary>
    /// Manage the connection with the Power BI REST API to manage a Push Dataset.
    /// The push dataset is created starting from a Tabular Object Model (TOM) structure, removing the 
    /// unsupported features (like user hierarchies and inactive relationships).
    /// The push dataset can be initialized reading data from another Power BI dataset.
    /// The class handles service principal authentication and user authentication for the refresh operation.
    /// The class also includes a simulator that creates and write rows according to a configuration table.
    /// </summary>
    public class PbiConnection
    {
        // Power BI API settings
        private const string resource = "https://analysis.windows.net/powerbi/api";
        private const string ApiUrl = "https://api.powerbi.com";

        /// <summary>
        /// The Tenant ID of Azure Active Directory. See https://docs.microsoft.com/en-us/azure/active-directory/fundamentals/active-directory-how-to-find-tenant
        /// </summary>
        public string TenantId { get; set; }

        /// <summary>
        /// This is the service principal for the Power BI REST API authentication. 
        /// Usually, the tool runs in a scheduled unattended batch. 
        /// The service principal is a string with a global unique identifier. 
        /// For more details about the service principal, read https://www.sqlbi.com/articles/creating-a-service-principal-account-for-power-bi-api/
        /// </summary>
        public string PrincipalId { get; set; }

        /// <summary>
        /// The client secret for the service principal. 
        /// For more details about the client secret, read https://www.sqlbi.com/articles/creating-a-service-principal-account-for-power-bi-api/
        /// </summary>
        public string ClientSecret { get; set; }

        // Properties for user authentication (required for Refresh in PbiPushDataset)

        /// <summary>
        /// The Client ID of the application registered in Azure Active Directory 
        /// that calls the REST API to update the push dataset. 
        /// You can see how to register an application and obtain a Client ID in https://www.sqlbi.com/articles/creating-a-service-principal-account-for-power-bi-api/
        /// </summary>
        public string ClientId { get; set; }

        /// <summary>
        /// The username to retrieve data from the regular dataset to initialize the push dataset after the daily refresh.
        /// </summary>
        public string Username { get; set; }

        /// <summary>
        /// The password to retrieve data from the regular dataset to initialize the push dataset after the daily refresh.
        /// </summary>
        public string Password { get; set; }

        public PbiConnection()
        {
        }

        private PowerBIClient _powerBIClient;

        /// <summary>
        /// Returns true if the connection to Power BI is active and already authenticated.
        /// </summary>
        public bool IsOpen { get => _powerBIClient != null; }

        /// <summary>
        /// Returns the access token for the service principal
        /// </summary>
        /// <returns></returns>
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

        /// <summary>
        /// Returns the access token for the user-based authentication.
        /// </summary>
        /// <param name="serverResource"></param>
        /// <returns></returns>
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


        /// <summary>
        /// Retrieves the access token for the service principal and connects to Power BI service.
        /// </summary>
        /// <returns></returns>
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

            #endregion

            var tokenCredentials = new TokenCredentials(result.AccessToken, "Bearer");
            _powerBIClient = new PowerBIClient(new Uri(ApiUrl), tokenCredentials);
        }

        /// <summary>
        /// Creates a push dataset in the specified workspace using the TOM model retrieved from a model.bim file.
        /// </summary>
        /// <param name="model">Filename containing the TOM model in model.bim forma (JSON).</param>
        /// <param name="groupId">The Group ID corresponding to the Power BI workspace where you publish the push dataset.</param>
        /// <param name="datasetName">The name of the push dataset to create.</param>
        /// <param name="overwriteExistingDataset">TRUE to overwrite an existing dataset, FALSE to raise an exception if the dataset already exists.</param>
        /// <param name="fifoPolicy">TRUE to use the BasicFIFO retention policy for the new push dataset, FALSE to use None as a retention policy.</param>
        /// <param name="unsupportedMeasureAction">Action to run for unsupported measures found in the model.</param>
        /// <param name="unsupportedRelationshipAction">Action to run for unsupported relationships found in the model.</param>
        /// <returns>Dataset ID of the push dataset.</returns>
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
                    throw new PbiPushDatasetException(this, $"Dataset {datasetName} already existing.");
                }
            }

            string modelBim = File.ReadAllText(model.FullName);
            TabModel.Database database = TabModel.JsonSerializer.DeserializeDatabase(modelBim);
            var schema = SchemaBuilder.GetDatasetRequest(datasetName, database.Model, unsupportedMeasureAction, unsupportedRelationshipAction);

            ds = await _powerBIClient.Datasets.PostDatasetInGroupAsync(
                groupId,
                schema,
                fifoPolicy ? DefaultRetentionPolicy.BasicFIFO : DefaultRetentionPolicy.None
            );

            return ds.Id;
        }

        /// <summary>
        /// Updates the structure of a push dataset in the specified workspace using the TOM model retrieved from a model.bim file.
        /// </summary>
        /// <param name="model">Filename containing the TOM model in model.bim forma (JSON).</param>
        /// <param name="groupId">The Group ID corresponding to the Power BI workspace containing the push dataset.</param>
        /// <param name="datasetName">The name of the push dataset to update.</param>
        /// <param name="unsupportedMeasureAction">Action to run for unsupported measures found in the model.</param>
        /// <param name="unsupportedRelationshipAction">Action to run for unsupported relationships found in the model.</param>
        /// <returns>List of updated tables in the push dataset.</returns>
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

        /// <summary>
        /// Updates the structure of a push dataset in the specified workspace using the TOM model retrieved from a model.bim file.
        /// </summary>
        /// <param name="model">Filename containing the TOM model in model.bim forma (JSON).</param>
        /// <param name="groupId">The Group ID corresponding to the Power BI workspace containing the push dataset.</param>
        /// <param name="datasetId">The dataset ID of the push dataset to update.</param>
        /// <param name="unsupportedMeasureAction">Action to run for unsupported measures found in the model.</param>
        /// <param name="unsupportedRelationshipAction">Action to run for unsupported relationships found in the model.</param>
        /// <returns>List of updated tables in the push dataset.</returns>
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

        /// <summary>
        /// Remove all the rows from one or all the tables of a push dataset.
        /// </summary>
        /// <param name="groupId">The Group ID corresponding to the Power BI workspace containing the push dataset.</param>
        /// <param name="datasetName">The name of the push dataset.</param>
        /// <param name="clearingTable">The table to clear. Pass null to clear all the tables.</param>
        /// <returns>List of tables cleared in the push dataset.</returns>
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

        /// <summary>
        /// Remove all the rows from one or all the tables of a push dataset
        /// </summary>
        /// <param name="groupId">The Group ID corresponding to the Power BI workspace containing the push dataset.</param>
        /// <param name="datasetId">The push dataset ID.</param>
        /// <param name="clearingTable">The table to clear. Pass null to clear all the tables.</param>
        /// <returns>List of tables cleared in the push dataset.</returns>
        public async Task<List<string>> ClearPushDataset(Guid groupId, Guid datasetId, Action<string> clearingTable = null)
        {
            await Open();
            var clearedTables = new List<string>();
            var tables = await _powerBIClient.Datasets.GetTablesInGroupAsync(groupId, datasetId.ToString());
            foreach (var t in tables.Value)
            {
                clearingTable?.Invoke(t.Name);
                await _powerBIClient.Datasets.DeleteRowsInGroupAsync(groupId, datasetId.ToString(), t.Name);
                clearedTables?.Add(t.Name);
            }
            return clearedTables;
        }

        /// <summary>
        /// Runs a simulation writing rows in one or more tables of a push dataset at intervals specified by the simulator configuration.
        /// </summary>
        /// <param name="groupId">The Group ID corresponding to the Power BI workspace containing the push dataset.</param>
        /// <param name="datasetName">The name of the push dataset.</param>
        /// <param name="simulator">Configuration for the simulation.</param>
        /// <returns>List of tables and number of rows written for each table.</returns>
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

        /// <summary>
        /// Runs a simulation writing rows in one or more tables of a push dataset at intervals specified by the simulator configuration.
        /// </summary>
        /// <param name="groupId">The Group ID corresponding to the Power BI workspace containing the push dataset.</param>
        /// <param name="datasetId">The push dataset ID.</param>
        /// <param name="simulator">Configuration for the simulation.</param>
        /// <returns>List of tables and number of rows written for each table.</returns>
        public async Task<List<(string, int)>> PushSimulation(Guid groupId, Guid datasetId, Simulator simulator)
        {
            await Open();

            var pushedTables = new List<(string, int)>();
            foreach (var table in simulator.Parameters.Tables)
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

        /// <summary>
        /// Writes in tables of a push dataset the result obtained by running one or more DAX queries
        /// on another dataset published on the same Power BI tenant (also on a different workspace).
        /// </summary>
        /// <param name="groupId">The Group ID corresponding to the Power BI workspace containing the push dataset.</param>
        /// <param name="datasetName">The name of the push dataset to update.</param>
        /// <param name="sourceWorkspace">The name of the workspace containing the dataset to use as a "data source".</param>
        /// <param name="sourceDatabase">The name of the dataset to use as a "data source".</param>
        /// <param name="dax">DAX queries to run over the source dataset.</param>
        /// <param name="refreshingTable">Action to execute while updating the tables in the push dataset.</param>
        /// <param name="clearTable">TRUE to overwrite destination tables in the push dataset, FALSE to append rows to existing data in the push dataset.</param>
        /// <returns>List of tables and number of rows written for each table.</returns>
        public async Task<List<(string, int)>> RefreshWithDax(
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

        /// <summary>
        /// Writes in tables of a push dataset the result obtained by running one or more DAX queries
        /// on another dataset published on the same Power BI tenant (also on a different workspace).
        /// </summary>
        /// <param name="groupId">The Group ID corresponding to the Power BI workspace containing the push dataset.</param>
        /// <param name="datasetId">The Dataset ID of the push dataset to update.</param>
        /// <param name="sourceWorkspace">The name of the workspace containing the dataset to use as a "data source".</param>
        /// <param name="sourceDatabase">The name of the dataset to use as a "data source".</param>
        /// <param name="dax">DAX queries to run over the source dataset.</param>
        /// <param name="refreshingTable">Action to execute while updating the tables in the push dataset.</param>
        /// <param name="clearTable">TRUE to overwrite destination tables in the push dataset, FALSE to append rows to existing data in the push dataset.</param>
        /// <returns>List of tables and number of rows written for each table.</returns>
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

        /// <summary>
        /// Writes in tables of a push dataset the result obtained by running one or more queries
        /// on an ADOMD connection. For example, it can be used to run queries over Analysis Services instances.
        /// </summary>
        /// <param name="groupId">The Group ID corresponding to the Power BI workspace containing the push dataset.</param>
        /// <param name="datasetId">The Dataset ID of the push dataset to update.</param>
        /// <param name="connectionString">ADOMD connection string for the source database.</param>
        /// <param name="query">Queries to run over the source database.</param>
        /// <param name="refreshingTable">Action to execute while updating the tables in the push dataset.</param>
        /// <param name="clearTable">TRUE to overwrite destination tables in the push dataset, FALSE to append rows to existing data in the push dataset.</param>
        /// <returns>List of tables and number of rows written for each table.</returns>
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
