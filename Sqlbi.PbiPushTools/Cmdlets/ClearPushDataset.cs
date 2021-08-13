using System;
using System.Collections.Generic;
using System.Management.Automation;
using System.CommandLine.Rendering;
using Sqlbi.PbiPushDataset;
using System.Threading.Tasks;

namespace Sqlbi.PbiPushTools.CmdLets
{
    [Cmdlet("Clear", "PushDataset")] // Correspond to Refresh request of command-line tool
    [OutputType(typeof(string))]
    public class ClearPushDataset : Cmdlet
    {
        [Parameter(Position = 0, Mandatory = true, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public string Principal { get; set; }

        [Parameter(Position = 1, Mandatory = true, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public string Secret { get; set; }

        [Parameter(Position = 2, Mandatory = true, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public string Tenant { get; set; }

        [Parameter(Position = 3, Mandatory = true, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public string Group { get; set; }

        [Parameter(Position = 4, Mandatory = false, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public string DatasetName { get; set; }

        [Parameter(Position = 5, Mandatory = false, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public string DatasetId { get; set; }

        protected override void ProcessRecord()
        {
            //base.ProcessRecord();

            WriteObject($"{Ansi.Color.Foreground.LightCyan}** CLEAR model **{Ansi.Color.Foreground.Default}");
            var pbiConnection = new PbiConnection
            {
                TenantId = Tenant,
                PrincipalId = Principal,
                ClientSecret = Secret
            };

            pbiConnection.Open().Wait();

            List<string> ClearedTables = new List<string>();

            var groupId = new Guid(Group);
            var clearedTables = (!string.IsNullOrWhiteSpace(DatasetId))
                ? pbiConnection.ClearPushDataset(groupId, new Guid(DatasetId), LogClearedTable).Result
                : pbiConnection.ClearPushDataset(groupId, DatasetName, LogClearedTable).Result;

            foreach ( var t in ClearedTables )
            {
                WriteObject($"{Ansi.Color.Foreground.LightGreen}  Cleared {t} {Ansi.Color.Foreground.Default}");
            }
            WriteObject($"{Ansi.Color.Foreground.LightCyan}Cleared {clearedTables?.Count} tables.{Ansi.Color.Foreground.Default}");

            void LogClearedTable(string t)
            {
                // Implementation issue - we cannot write output during async calls
                ClearedTables.Add(t);
            }
        }
    }
}