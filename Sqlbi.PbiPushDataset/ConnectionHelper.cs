using System;

namespace Sqlbi.PbiPushDataset
{
    class ConnectionHelper
    {
        const string MSOLAP_DRIVER_NAME = "MSOLAP"; // We get the latest available. We currently don't enforce MSOLAP.8

        internal enum AsInstanceType
        {
            Other,
            AsAzure,
            PbiDedicated,
            PbiPremium,
            PbiDataset
        }

        private static bool IsProtocolSchemeInstance(string dataSourceUri, string protocolScheme)
        {
            return dataSourceUri?.StartsWith(protocolScheme, StringComparison.InvariantCultureIgnoreCase) ?? false;
        }
        internal static AsInstanceType GetAsInstanceType(string dataSourceUri)
        {
            if (IsProtocolSchemeInstance(dataSourceUri, "asazure://"))
            {
                return AsInstanceType.AsAzure;
            }
            if (IsProtocolSchemeInstance(dataSourceUri, "pbidedicated://"))
            {
                return AsInstanceType.PbiDedicated;
            }
            if (IsProtocolSchemeInstance(dataSourceUri, "powerbi://"))
            {
                return AsInstanceType.PbiPremium;
            }
            if (IsProtocolSchemeInstance(dataSourceUri, "pbiazure://"))
            {
                return AsInstanceType.PbiDataset;
            }
            return AsInstanceType.Other;
        }


        /// <summary>
        /// Returns the OLE DB connection string based on serverName and databaseName
        /// </summary>
        /// <param name="serverName"></param>
        /// <param name="databaseName"></param>
        /// <returns>Connection string for OLE DB</returns>
        public static string GetOleDbConnectionString(string serverName, string databaseName)
        {
            string connectionString;
            switch (GetAsInstanceType(serverName))
            {
                case AsInstanceType.PbiDataset:
                    connectionString = $"Provider={MSOLAP_DRIVER_NAME};Persist Security Info=True;Initial Catalog=sobe_wowvirtualserver-{databaseName};Data Source={serverName};MDX Compatibility=1;Safety Options=2;MDX Missing Member Mode=Error;Identity Provider=https://login.microsoftonline.com/common, https://analysis.windows.net/powerbi/api, 929d0ec0-7a41-4b1e-bc7c-b754a28bddcc;Update Isolation Level=2";
                    break;
                case AsInstanceType.PbiDedicated:
                case AsInstanceType.PbiPremium:
                case AsInstanceType.AsAzure:
                    connectionString = $"Provider={MSOLAP_DRIVER_NAME};Persist Security Info=True;Data Source={serverName};Update Isolation Level=2;Initial Catalog={databaseName}";
                    break;
                default:
                    connectionString = $"Provider={MSOLAP_DRIVER_NAME};Integrated Security=SSPI;Persist Security Info=True;Data Source={serverName};Update Isolation Level=2;Initial Catalog={databaseName}";
                    break;
            }
            return connectionString;
        }

        public static string GetOleDbPBIConnectionString(string sourceWorkspace, string sourceDatabase)
        {
            var connStr = $"Data Source={sourceWorkspace};Catalog={sourceDatabase}";
            return connStr;
        }
    }
}
