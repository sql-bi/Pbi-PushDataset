using System.IO;
using System.Management.Automation;
using System.Collections.Generic;
using System.CommandLine.Rendering;
using TabModel = Microsoft.AnalysisServices.Tabular;
using Sqlbi.PbiPushDataset;

namespace Sqlbi.PbiPushTools.CmdLets
{
    [Cmdlet("New", "PushDataset")] // Correspond to Generate request of command-line tool
    [OutputType(typeof(string))]
    public class NewPushDataset : Cmdlet
    {
        [Parameter(Position = 0, Mandatory = true, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public FileInfo Model { get; set; }

        [Parameter(Position = 1, Mandatory = true, ValueFromPipeline = true, ValueFromPipelineByPropertyName = true)]
        [ValidateNotNullOrEmpty()]
        public FileInfo Out { get; set; }

        protected override void ProcessRecord()
        {
            base.ProcessRecord();

            WriteObject($"{Ansi.Color.Foreground.LightCyan}** GENERATE model **{Ansi.Color.Foreground.Default}");
            if (!Model.Exists)
            {
                WriteObject($"{Ansi.Color.Foreground.LightRed}Model file {Model?.FullName} not found.{Ansi.Color.Foreground.Default}");
                return;
            }
            WriteObject($"Loading model: {Model.FullName}");

            string modelBim = File.ReadAllText(Model.FullName);
            TabModel.Database database = TabModel.JsonSerializer.DeserializeDatabase(modelBim);

            // Create the schema
            if (!SchemaBuilder.CheckModel(
                database.Model,
                out List<TabModel.Table> unsupportedTables,
                out List<TabModel.Measure> unsupportedMeasures,
                out List<TabModel.Relationship> unsupportedRelationships))
            {
                if (unsupportedMeasures.Count > 0)
                {
                    WriteObject($"{Ansi.Color.Foreground.LightYellow}Removing {unsupportedMeasures.Count} unsupported measures.{Ansi.Color.Foreground.Default}");
                    foreach (TabModel.Measure m in unsupportedMeasures)
                    {
                        WriteObject($"  {Ansi.Color.Foreground.Yellow}{m.Name}{Ansi.Color.Foreground.Default}");
                        m.Table.Measures.Remove(m);
                    }
                }

                if (unsupportedRelationships.Count > 0)
                {
                    WriteObject($"{Ansi.Color.Foreground.LightYellow}Removing {unsupportedRelationships.Count} unsupported relationships.{Ansi.Color.Foreground.Default}");
                    foreach (TabModel.Relationship r in unsupportedRelationships)
                    {
                        string relationshipDescription = (r is TabModel.SingleColumnRelationship sr)
                                ? $"*** SKIP *** '{sr.FromTable.Name}'[{sr.FromColumn.Name}]{sr.CardinalityText()}'{sr.ToTable.Name}'[{sr.ToColumn.Name}] ({sr.CrossFilteringBehavior})"
                                : $"*** SKIP *** '{r.FromTable.Name}'-->'{r.ToTable.Name}' ({r.CrossFilteringBehavior})";
                        WriteObject($"  {Ansi.Color.Foreground.Yellow}{relationshipDescription}{Ansi.Color.Foreground.Default}");
                        database.Model.Relationships.Remove(r);
                    }
                }

                if (unsupportedTables.Count > 0)
                {
                    WriteObject($"{Ansi.Color.Foreground.LightYellow}Removing {unsupportedTables.Count} unsupported tables.{Ansi.Color.Foreground.Default}");
                    foreach (TabModel.Table t in unsupportedTables)
                    {
                        WriteObject($"  {Ansi.Color.Foreground.Yellow}{t.Name}{Ansi.Color.Foreground.Default}");
                        database.Model.Tables.Remove(t);
                    }
                }

                WriteObject($"{Ansi.Color.Foreground.LightCyan}Prepared model compatible with Push Dataset.{Ansi.Color.Foreground.Default}");
            }
            else
            {
                WriteObject($"{Ansi.Color.Foreground.LightGreen}Copying model compatible with Push Dataset.{Ansi.Color.Foreground.Default}");
            }

            string modelCompatibleBim = TabModel.JsonSerializer.SerializeDatabase(database);
            WriteObject($"Saving model: {Out.FullName}");
            File.WriteAllText(Out.FullName, modelCompatibleBim);
        }
    }
}