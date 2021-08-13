using System;
using System.IO;
using System.Management.Automation;
using System.Collections.Generic;
using System.CommandLine.Rendering;
using TabModel = Microsoft.AnalysisServices.Tabular;
using Sqlbi.PbiPushDataset;

namespace Sqlbi.PbiPushTools.CmdLets
{
    [Cmdlet("Update", "PushDataset")] // Correspond to Refresh request of command-line tool
    [OutputType(typeof(string))]
    public class UpdatePushDataset : Cmdlet
    {
        [Parameter(Position = 0, Mandatory = true, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public FileInfo Dax { get; set; }

        [Parameter(Position = 1, Mandatory = true, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public string Principal { get; set; }

        [Parameter(Position = 2, Mandatory = true, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public string Secret { get; set; }

        [Parameter(Position = 3, Mandatory = true, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public string Tenant { get; set; }

        [Parameter(Position = 4, Mandatory = true, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public string Group { get; set; }

        [Parameter(Position = 5, Mandatory = false, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public string DatasetName { get; set; }

        [Parameter(Position = 6, Mandatory = false, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public string DatasetId { get; set; }

        [Parameter(Position = 7, Mandatory = false, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public string ReadFromWorkspace { get; set; }

        [Parameter(Position = 8, Mandatory = false, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public string ReadFromDatabase { get; set; }

        [Parameter(Position = 9, Mandatory = false, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public string Client { get; set; }

        [Parameter(Position = 10, Mandatory = false, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public string Username { get; set; }

        [Parameter(Position = 11, Mandatory = false, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public string Password { get; set; }

        protected override void ProcessRecord()
        {
            base.ProcessRecord();

            WriteObject($"{Ansi.Color.Foreground.LightCyan}** REFRESH model executing {Dax?.Name} script**{Ansi.Color.Foreground.Default}");
            if (!Dax.Exists)
            {
                WriteObject($"{Ansi.Color.Foreground.LightRed}DAX file {Dax?.FullName} not found.{Ansi.Color.Foreground.Default}");
                return;
            }

            var pbiConnection = new PbiConnection
            {
                TenantId = Tenant,
                PrincipalId = Principal,
                ClientSecret = Secret,

                ClientId = Client,
                Username = Username,
                Password = Password
            };

            pbiConnection.Open().Wait();

            string daxQueries = File.ReadAllText(Dax.FullName);
            var groupId = new Guid(Group);
            var refreshTables = (!string.IsNullOrWhiteSpace(DatasetId))
                ? pbiConnection.RefreshWithDax(groupId, new Guid(DatasetId), ReadFromWorkspace, ReadFromDatabase, daxQueries, null).Result
                : pbiConnection.RefreshWithDax(groupId, DatasetName, ReadFromWorkspace, ReadFromDatabase, daxQueries, null).Result;

            WriteObject($"{Ansi.Color.Foreground.LightCyan}Refreshed {refreshTables?.Count} tables.{Ansi.Color.Foreground.Default}");
            foreach (var table in refreshTables)
            {
                WriteObject($"{Ansi.Color.Foreground.Cyan}    {table.Item1} ({table.Item2} rows){Ansi.Color.Foreground.Default}");
            }

            /*
             * Async calls to WriteObject not supported
             * LogRefresh could be used as refreshingTable argument 
             * of RefreshWithDax to display refresh progress
             * 
            void LogRefresh(string tableName, int rows)
            {
                if (rows < 0)
                {
                    WriteObject($"{Ansi.Color.Foreground.Green}  Refreshing {tableName}  {Ansi.Color.Foreground.Default}");
                }
                else
                {
                    WriteObject($"{Ansi.Color.Foreground.LightGreen}  Written {rows} rows to {tableName}  {Ansi.Color.Foreground.Default}");
                }
            }
            */
        }
    }
}