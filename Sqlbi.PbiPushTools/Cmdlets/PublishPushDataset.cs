using System;
using System.IO;
using System.Collections.Generic;
using System.Management.Automation;
using System.CommandLine.Rendering;
using TabModel = Microsoft.AnalysisServices.Tabular;
using Sqlbi.PbiPushDataset;

namespace Sqlbi.PbiPushTools.CmdLets
{
    [Cmdlet("Publish", "PushDataset")] // Correspond to Publish request of command-line tool
    [OutputType(typeof(string))]
    public class PublishPushDataset : Cmdlet
    {
        [Parameter(Position = 0, Mandatory = true, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public FileInfo Model { get; set; }

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

        protected override void ProcessRecord()
        {
            base.ProcessRecord();
            WriteObject($"{Ansi.Color.Foreground.LightCyan}** PUBLISH model **{Ansi.Color.Foreground.Default}");
            if (!Model.Exists)
            {
                WriteObject($"{Ansi.Color.Foreground.LightRed}Model file {Model?.FullName} not found.{Ansi.Color.Foreground.Default}");
                return;
            }

            var pbiConnection = new PbiConnection
            {
                TenantId = Tenant,
                PrincipalId = Principal,
                ClientSecret = Secret
            };

            var unsupportedMeasures = new List<TabModel.Measure>();
            var unsupportedRelationships = new List<TabModel.Relationship>();
            var groupId = new Guid(Group);
            string newDatasetId = pbiConnection.CreatePushDataset(Model, groupId, DatasetName, false, false, DumpUnsupportedMeasure, DumpUnsupportedRelationship).Result;

            foreach (var unsupportedMeasure in unsupportedMeasures)
            {
                WriteObject($"{Ansi.Color.Foreground.LightYellow}Unsupported measure: {Ansi.Color.Foreground.Yellow}{ unsupportedMeasure.Name} {Ansi.Color.Foreground.Default}");
            }

            foreach (var unsupportedRelationship in unsupportedRelationships)
            {
                WriteObject($"{Ansi.Color.Foreground.LightYellow}Unsupported relationship: {Ansi.Color.Foreground.Yellow}");
                WriteObject(
                    (unsupportedRelationship is TabModel.SingleColumnRelationship sr)
                        ? $"'{sr.FromTable.Name}'[{sr.FromColumn.Name}]{sr.CardinalityText()}'{sr.ToTable.Name}'[{sr.ToColumn.Name}] ({sr.CrossFilteringBehavior})"
                        : unsupportedRelationship.ToString()
                );
                WriteObject($"{Ansi.Color.Foreground.Default}");
            }

            WriteObject($"{Ansi.Color.Foreground.LightCyan}Created dataset id={newDatasetId}{Ansi.Color.Foreground.Default}");

            void DumpUnsupportedMeasure(TabModel.Measure unsupportedMeasure)
            {
                unsupportedMeasures.Add(unsupportedMeasure);
            }

            void DumpUnsupportedRelationship(TabModel.Relationship unsupportedRelationship)
            {
                unsupportedRelationships.Add(unsupportedRelationship);
            }
        }
    }
}