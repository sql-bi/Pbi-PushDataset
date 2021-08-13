using System.IO;
using System.Management.Automation;
using System.Collections.Generic;
using System.CommandLine.Rendering;
using TabModel = Microsoft.AnalysisServices.Tabular;
using Sqlbi.PbiPushDataset;

namespace Sqlbi.PbiPushTools.CmdLets
{
    [Cmdlet("Confirm", "PushDataset")] // Correspond to Check request of command-line tool
    [OutputType(typeof(string))]
    public class ConfirmPushDataset : Cmdlet
    {
        [Parameter(Position = 0, Mandatory = true, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public FileInfo Model { get; set; }

        protected override void ProcessRecord()
        {
            base.ProcessRecord();

            if (!Model.Exists)
            {
                WriteObject($"{Ansi.Color.Foreground.LightRed}Model file {Model?.FullName} not found.{Ansi.Color.Foreground.Default}");
                return;
            }
            WriteObject($"Checking model: {Model.FullName}");

            string modelBim = File.ReadAllText(Model.FullName);
            TabModel.Database database = TabModel.JsonSerializer.DeserializeDatabase(modelBim);

            // Create the schema
            if (!SchemaBuilder.CheckModel(
                database.Model,
                out List<TabModel.Table> unsupportedTables,
                out List<TabModel.Measure> unsupportedMeasures,
                out List<TabModel.Relationship> unsupportedRelationships))
            {
                void DumpUnsupportedElement<T>(List<T> unsupportedList, string name)
                {
                    if (unsupportedList.Count > 0)
                    {
                        WriteObject($"{Ansi.Color.Foreground.LightYellow}{unsupportedList.Count} {name} are not supported:{Ansi.Color.Foreground.Default}");
                        foreach (var item in unsupportedList)
                        {
                            string itemName =
                                (item is TabModel.Table) ? (item as TabModel.Table).Name
                                : (item is TabModel.Measure) ? (item as TabModel.Measure).Name
                                : (item is TabModel.SingleColumnRelationship sr) ? $"'{sr.FromTable.Name}'[{sr.FromColumn.Name}]{sr.CardinalityText()}'{sr.ToTable.Name}'[{sr.ToColumn.Name}] ({sr.CrossFilteringBehavior})"
                                : item.ToString();
                            WriteObject($"  {Ansi.Color.Foreground.Yellow}{itemName}{Ansi.Color.Foreground.Default}");
                        }
                    }
                }

                DumpUnsupportedElement(unsupportedTables, "tables");
                DumpUnsupportedElement(unsupportedMeasures, "measures");
                DumpUnsupportedElement(unsupportedRelationships, "relationships");

                WriteObject($"{Ansi.Color.Foreground.LightRed}Model has unsupported objects.{Ansi.Color.Foreground.Default}");
            }
            else
            {
                WriteObject($"{Ansi.Color.Foreground.LightGreen}Model validated.{Ansi.Color.Foreground.Default}");
            }

        }
    }
}