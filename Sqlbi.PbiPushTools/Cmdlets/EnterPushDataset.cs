using System;
using System.IO;
using System.Management.Automation;
using System.CommandLine.Rendering;
using Sqlbi.PbiPushDataset;

namespace Sqlbi.PbiPushTools.CmdLets
{
    [Cmdlet("Enter", "PushDataset")] // Correspond to Simulate request of command-line tool
    [OutputType(typeof(string))]
    public class EnterPushDataset : Cmdlet
    {
        [Parameter(Position = 0, Mandatory = true, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public FileInfo Configuration { get; set; }

        [Parameter(Position = 1, Mandatory = true, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public string Principal { get; set; }

        [Parameter(Position = 2, Mandatory = true, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public string Secret { get; set; }

        [Parameter(Position = 3, Mandatory = true, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public string Tenant{ get; set; }

        [Parameter(Position = 4, Mandatory = true, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public string Group { get; set; }

        [Parameter(Position = 5, Mandatory = false, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public string DatasetName { get; set; }

        [Parameter(Position = 6, Mandatory = false, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public string DatasetId { get; set; }

        protected override void ProcessRecord()
        {
            base.ProcessRecord();

            WriteObject($"{Ansi.Color.Foreground.LightCyan}** SIMULATE real-time updates using {Configuration?.Name} configuration**{Ansi.Color.Foreground.Default}");
            
            if (!Configuration.Exists)
            {
                WriteObject($"{Ansi.Color.Foreground.LightRed}Configuration file {Configuration?.FullName} not found.{Ansi.Color.Foreground.Default}");
                return;
            }

            Simulator simulator = Simulator.ReadParameters(Configuration.FullName);

            var pbiConnection = new PbiConnection
            {
                TenantId = Tenant,
                PrincipalId = Principal,
                ClientSecret = Secret
            };

            pbiConnection.Open().Wait();
            var groupId = new Guid(Group);

            bool loopSimulation = true;
            do
            {
                var pushedTables = (!string.IsNullOrWhiteSpace(DatasetId))
                    ? pbiConnection.PushSimulation(groupId, new Guid(DatasetId), simulator).Result
                    : pbiConnection.PushSimulation(groupId, DatasetName, simulator).Result;

                WriteObject($"{Ansi.Color.Foreground.LightCyan}Pushed data in {pushedTables?.Count} tables.{Ansi.Color.Foreground.Default}");
                foreach (var table in pushedTables)
                {
                    WriteObject($"{Ansi.Color.Foreground.Cyan}    {table.Item1} ({table.Item2} rows){Ansi.Color.Foreground.Default}");
                }

                WriteObject($"Waiting {simulator.Parameters.BatchInterval} seconds - press X to stop.");
                System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
                sw.Start();
                while (sw.ElapsedMilliseconds < simulator.Parameters.BatchInterval * 1000)
                {
                    if (Stopping || Console.KeyAvailable && (Console.ReadKey(true).Key == ConsoleKey.X))
                    {
                        loopSimulation = false;
                        break;
                    }
                    System.Threading.Thread.Sleep(250);
                }
            }
            while (loopSimulation);

            WriteObject($"{Ansi.Color.Foreground.White}Simulation stopped.{Ansi.Color.Foreground.Default}");
        }
    }
}