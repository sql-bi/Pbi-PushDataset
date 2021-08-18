using System;
using System.Collections.Generic;
using System.CommandLine;
using System.CommandLine.Invocation;
using System.IO;
using System.CommandLine.Rendering;
using Microsoft.AnalysisServices.Tabular;

using Sqlbi.PbiPushDataset;
using TabModel = Microsoft.AnalysisServices.Tabular;

namespace Sqlbi.PbiPushTools
{
    public static class Helper
    {
        public static string CardinalityText(this SingleColumnRelationship sr)
        {
            return $" {((sr.FromCardinality == RelationshipEndCardinality.Many) ? '*' : '1')}--{ ((sr.ToCardinality == RelationshipEndCardinality.Many) ? '*' : '1')} ";
        }

    }

    class CommandLine
    {
        static void Publish(FileInfo model, string principal, string secret, string tenant, string group, string datasetName, string datasetId)
        {
            Console.WriteLine($"{Ansi.Color.Foreground.LightCyan}** PUBLISH model **{Ansi.Color.Foreground.Default}");
            if (!model.Exists)
            {
                Console.WriteLine($"{Ansi.Color.Foreground.LightRed}Model file {model?.FullName} not found.{Ansi.Color.Foreground.Default}");
                return;
            }

            var pbiConnection = new PbiConnection
            {
                TenantId = tenant,
                PrincipalId = principal,
                ClientSecret = secret
            };

            var groupId = new Guid(group);
            string newDatasetId = pbiConnection.CreatePushDataset(model, groupId, datasetName, false, false, DumpUnsupportedMeasure, DumpUnsupportedRelationship).Result;

            Console.WriteLine($"{Ansi.Color.Foreground.LightCyan}Created dataset id={newDatasetId}{Ansi.Color.Foreground.Default}");

            static void DumpUnsupportedMeasure(TabModel.Measure unsupportedMeasure)
            {
                Console.WriteLine($"{Ansi.Color.Foreground.LightYellow}Unsupported measure: {Ansi.Color.Foreground.Yellow}{ unsupportedMeasure.Name} {Ansi.Color.Foreground.Default}");
            }

            static void DumpUnsupportedRelationship(TabModel.Relationship unsupportedRelationship)
            {
                Console.Write($"{Ansi.Color.Foreground.LightYellow}Unsupported relationship: {Ansi.Color.Foreground.Yellow}");
                Console.Write(
                    (unsupportedRelationship is SingleColumnRelationship sr) 
                        ? $"'{sr.FromTable.Name}'[{sr.FromColumn.Name}]{sr.CardinalityText()}'{sr.ToTable.Name}'[{sr.ToColumn.Name}] ({sr.CrossFilteringBehavior})"
                        : unsupportedRelationship.ToString()
                );
                Console.WriteLine($"{Ansi.Color.Foreground.Default}");
            }
        }

        static void Alter(FileInfo model, string principal, string secret, string tenant, string group, string datasetName, string datasetId)
        {
            Console.WriteLine($"{Ansi.Color.Foreground.LightCyan}** ALTER model **{Ansi.Color.Foreground.Default}");
            if (!model.Exists)
            {
                Console.WriteLine($"{Ansi.Color.Foreground.LightRed}Model file {model?.FullName} not found.{Ansi.Color.Foreground.Default}");
                return;
            }

            var pbiConnection = new PbiConnection
            {
                TenantId = tenant,
                PrincipalId = principal,
                ClientSecret = secret
            };

            var groupId = new Guid(group);
            var alteredTables = pbiConnection.AlterPushDataset(model, groupId, datasetName, DumpUnsupportedMeasure, DumpUnsupportedRelationship).Result;

            foreach (var table in alteredTables)
            {
                Console.WriteLine($"{Ansi.Color.Foreground.LightCyan}Altered table {table}{Ansi.Color.Foreground.Default}");
            }

            static void DumpUnsupportedMeasure(TabModel.Measure unsupportedMeasure)
            {
                Console.WriteLine($"{Ansi.Color.Foreground.LightYellow}Unsupported measure: {Ansi.Color.Foreground.Yellow}{ unsupportedMeasure.Name} {Ansi.Color.Foreground.Default}");
            }

            static void DumpUnsupportedRelationship(TabModel.Relationship unsupportedRelationship)
            {
                Console.Write($"{Ansi.Color.Foreground.LightYellow}Unsupported relationship: {Ansi.Color.Foreground.Yellow}");
                Console.Write(
                    (unsupportedRelationship is SingleColumnRelationship sr)
                        ? $"'{sr.FromTable.Name}'[{sr.FromColumn.Name}]{sr.CardinalityText()}'{sr.ToTable.Name}'[{sr.ToColumn.Name}] ({sr.CrossFilteringBehavior})"
                        : unsupportedRelationship.ToString()
                );
                Console.WriteLine($"{Ansi.Color.Foreground.Default}");
            }
        }

        static void Refresh(
            FileInfo dax, string principal, string secret, string tenant, 
            string group, string datasetName, 
            // string datasetId,  -- Commented to not use more than 11 arguments
            string readFromWorkspace, string readFromDatabase,
            string userApp, string userName, string userPassword )
        {
            string datasetId = null; // TODO remove this line if we restore datasetId argument

            Console.WriteLine($"{Ansi.Color.Foreground.LightCyan}** REFRESH model executing {dax?.Name} script**{Ansi.Color.Foreground.Default}");
            if (!dax.Exists)
            {
                Console.WriteLine($"{Ansi.Color.Foreground.LightRed}DAX file {dax?.FullName} not found.{Ansi.Color.Foreground.Default}");
                return;
            }

            var pbiConnection = new PbiConnection
            {
                TenantId = tenant,
                PrincipalId = principal,
                ClientSecret = secret,

                ClientId = userApp,
                Username = userName,
                Password = userPassword
            };

            pbiConnection.Open().Wait();

            string daxQueries = File.ReadAllText(dax.FullName);
            var groupId = new Guid(group);
            try
            {
                var refreshTables = (!string.IsNullOrWhiteSpace(datasetId))
                    ? pbiConnection.RefreshWithDax(groupId, new Guid(datasetId), readFromWorkspace, readFromDatabase, daxQueries, LogRefresh).Result
                    : pbiConnection.RefreshWithDax(groupId, datasetName, readFromWorkspace, readFromDatabase, daxQueries, LogRefresh).Result;

                Console.WriteLine($"{Ansi.Color.Foreground.LightCyan}Refreshed {refreshTables?.Count} tables.{Ansi.Color.Foreground.Default}");
                foreach (var table in refreshTables)
                {
                    Console.WriteLine($"{Ansi.Color.Foreground.Cyan}    {table.Item1} ({table.Item2} rows){Ansi.Color.Foreground.Default}");
                }
            }
            catch ( Exception ex )
            {
                Console.WriteLine(ex.Message);
                throw;
            }

            static void LogRefresh(string tableName, int rows)
            {
                if (rows < 0)
                {
                    Console.WriteLine($"{Ansi.Color.Foreground.Green}  Refreshing {tableName}  {Ansi.Color.Foreground.Default}");
                }
                else
                {
                    Console.WriteLine($"{Ansi.Color.Foreground.LightGreen}  Written {rows} rows to {tableName}  {Ansi.Color.Foreground.Default}");
                }
            }
        }

        static void Clear(string principal, string secret, string tenant, string group, string datasetName, string datasetId)
        {
            Console.WriteLine($"{Ansi.Color.Foreground.LightCyan}** CLEAR model **{Ansi.Color.Foreground.Default}");
            var pbiConnection = new PbiConnection
            {
                TenantId = tenant,
                PrincipalId = principal,
                ClientSecret = secret
            };

            pbiConnection.Open().Wait();

            var groupId = new Guid(group);
            var clearedTables = (!string.IsNullOrWhiteSpace(datasetId))
                ? pbiConnection.ClearPushDataset(groupId, new Guid(datasetId), LogClearedTable).Result
                : pbiConnection.ClearPushDataset(groupId, datasetName, LogClearedTable).Result;

            Console.WriteLine($"{Ansi.Color.Foreground.LightCyan}Cleared {clearedTables?.Count} tables.{Ansi.Color.Foreground.Default}");

            static void LogClearedTable( string t )
            {
                Console.WriteLine($"{Ansi.Color.Foreground.LightGreen}  Clearing {t} {Ansi.Color.Foreground.Default}");
            }
        }

        static void Simulate(FileInfo configuration, string principal, string secret, string tenant, string group, string datasetName, string datasetId)
        {
            Console.WriteLine($"{Ansi.Color.Foreground.LightCyan}** SIMULATE real-time updates using {configuration?.Name} configuration**{Ansi.Color.Foreground.Default}");
            if (!configuration.Exists)
            {
                Console.WriteLine($"{Ansi.Color.Foreground.LightRed}Configuration file {configuration?.FullName} not found.{Ansi.Color.Foreground.Default}");
                return;
            }

            Simulator simulator = Simulator.ReadParameters(configuration.FullName);

            var pbiConnection = new PbiConnection
            {
                TenantId = tenant,
                PrincipalId = principal,
                ClientSecret = secret
            };

            pbiConnection.Open().Wait();
            var groupId = new Guid(group);

            bool loopSimulation = true;
            do
            {
                var pushedTables = (!string.IsNullOrWhiteSpace(datasetId))
                    ? pbiConnection.PushSimulation(groupId, new Guid(datasetId), simulator).Result
                    : pbiConnection.PushSimulation(groupId, datasetName, simulator).Result;

                Console.WriteLine($"{Ansi.Color.Foreground.LightCyan}Pushed data in {pushedTables?.Count} tables.{Ansi.Color.Foreground.Default}");
                foreach (var table in pushedTables)
                {
                    Console.WriteLine($"{Ansi.Color.Foreground.Cyan}    {table.Item1} ({table.Item2} rows){Ansi.Color.Foreground.Default}");
                }

                Console.WriteLine($"Waiting {simulator.Parameters.BatchInterval} seconds - press X to stop.");
                System.Diagnostics.Stopwatch sw = new System.Diagnostics.Stopwatch();
                sw.Start();
                while (sw.ElapsedMilliseconds < simulator.Parameters.BatchInterval * 1000)
                {
                    if (Console.KeyAvailable && (Console.ReadKey(true).Key == ConsoleKey.X))
                    {
                        loopSimulation = false;
                        break;
                    }
                    System.Threading.Thread.Sleep(250);
                }
            }
            while (loopSimulation);

            Console.WriteLine($"{Ansi.Color.Foreground.White}Simulation stopped.{Ansi.Color.Foreground.Default}");
        }

        static void Check( FileInfo model )
        {
            Console.WriteLine($"{Ansi.Color.Foreground.LightCyan}** CHECK model **{Ansi.Color.Foreground.Default}");
            if (!model.Exists)
            {
                Console.WriteLine($"{Ansi.Color.Foreground.LightRed}Model file {model?.FullName} not found.{Ansi.Color.Foreground.Default}");
                return;
            }
            Console.WriteLine($"Checking model: {model.FullName}");

            string modelBim = File.ReadAllText(model.FullName);
            Database database = JsonSerializer.DeserializeDatabase(modelBim);

            // Create the schema
            if (!SchemaBuilder.CheckModel(
                database.Model, 
                out List<Table> unsupportedTables,
                out List<Measure> unsupportedMeasures,
                out List<Relationship> unsupportedRelationships))
            {
                static void DumpUnsupportedElement<T>( List<T> unsupportedList, string name )
                {
                    if (unsupportedList.Count > 0)
                    {
                        Console.WriteLine($"{Ansi.Color.Foreground.LightYellow}{unsupportedList.Count} {name} are not supported:{Ansi.Color.Foreground.Default}");
                        foreach (var item in unsupportedList)
                        {
                            string itemName =
                                (item is Table) ? (item as Table).Name
                                : (item is Measure) ? (item as Measure).Name
                                : (item is SingleColumnRelationship sr) ? $"'{sr.FromTable.Name}'[{sr.FromColumn.Name}]{sr.CardinalityText()}'{sr.ToTable.Name}'[{sr.ToColumn.Name}] ({sr.CrossFilteringBehavior})"
                                : item.ToString();
                            Console.WriteLine($"  {Ansi.Color.Foreground.Yellow}{itemName}{Ansi.Color.Foreground.Default}");
                        }
                    }
                }

                DumpUnsupportedElement(unsupportedTables, "tables");
                DumpUnsupportedElement(unsupportedMeasures, "measures");
                DumpUnsupportedElement(unsupportedRelationships, "relationships");

                Console.WriteLine($"{Ansi.Color.Foreground.LightRed}Model has unsupported objects.{Ansi.Color.Foreground.Default}");
            }
            else
            {
                Console.WriteLine($"{Ansi.Color.Foreground.LightGreen}Model validated.{Ansi.Color.Foreground.Default}");
            }
        }
        static RootCommand rootCommand;

        static void Generate(FileInfo model, FileInfo @out)
        {
            Console.WriteLine($"{Ansi.Color.Foreground.LightCyan}** GENERATE model **{Ansi.Color.Foreground.Default}");
            if (!model.Exists)
            {
                Console.WriteLine($"{Ansi.Color.Foreground.LightRed}Model file {model?.FullName} not found.{Ansi.Color.Foreground.Default}");
                return;
            }
            Console.WriteLine($"Loading model: {model.FullName}");

            string modelBim = File.ReadAllText(model.FullName);
            Database database = JsonSerializer.DeserializeDatabase(modelBim);

            // Create the schema
            if (!SchemaBuilder.CheckModel(
                database.Model,
                out List<Table> unsupportedTables,
                out List<Measure> unsupportedMeasures,
                out List<Relationship> unsupportedRelationships))
            {
                if (unsupportedMeasures.Count > 0)
                {
                    Console.WriteLine($"{Ansi.Color.Foreground.LightYellow}Removing {unsupportedMeasures.Count} unsupported measures.{Ansi.Color.Foreground.Default}");
                    foreach (Measure m in unsupportedMeasures)
                    {
                        Console.WriteLine($"  {Ansi.Color.Foreground.Yellow}{m.Name}{Ansi.Color.Foreground.Default}");
                        m.Table.Measures.Remove(m);
                    }
                }

                if (unsupportedRelationships.Count > 0)
                {
                    Console.WriteLine($"{Ansi.Color.Foreground.LightYellow}Removing {unsupportedRelationships.Count} unsupported relationships.{Ansi.Color.Foreground.Default}");
                    foreach (Relationship r in unsupportedRelationships)
                    {
                        string relationshipDescription = (r is SingleColumnRelationship sr)
                                ? $"*** SKIP *** '{sr.FromTable.Name}'[{sr.FromColumn.Name}]{sr.CardinalityText()}'{sr.ToTable.Name}'[{sr.ToColumn.Name}] ({sr.CrossFilteringBehavior})"
                                : $"*** SKIP *** '{r.FromTable.Name}'-->'{r.ToTable.Name}' ({r.CrossFilteringBehavior})";
                        Console.WriteLine($"  {Ansi.Color.Foreground.Yellow}{relationshipDescription}{Ansi.Color.Foreground.Default}");
                        database.Model.Relationships.Remove(r);
                    }
                }

                if (unsupportedTables.Count > 0)
                {
                    Console.WriteLine($"{Ansi.Color.Foreground.LightYellow}Removing {unsupportedTables.Count} unsupported tables.{Ansi.Color.Foreground.Default}");
                    foreach (Table t in unsupportedTables)
                    {
                        Console.WriteLine($"  {Ansi.Color.Foreground.Yellow}{t.Name}{Ansi.Color.Foreground.Default}");
                        database.Model.Tables.Remove(t);
                    }
                }

                Console.WriteLine($"{Ansi.Color.Foreground.LightCyan}Prepared model compatible with Push Dataset.{Ansi.Color.Foreground.Default}");
            }
            else
            {
                Console.WriteLine($"{Ansi.Color.Foreground.LightGreen}Copying model compatible with Push Dataset.{Ansi.Color.Foreground.Default}");
            }

            string modelCompatibleBim = JsonSerializer.SerializeDatabase(database);
            Console.WriteLine($"Saving model: {@out.FullName}");
            File.WriteAllText(@out.FullName, modelCompatibleBim);
        }

        static int Main(string[] args)
        {
            var checkCommand = new Command("check", "Validate the model for a Push Dataset" )
            {
                new Option<FileInfo>(
                    new string [] { "-model", "-m" },
                    "Model file to check (.bim)") { IsRequired = true }
            };
            checkCommand.Handler = CommandHandler.Create<FileInfo>(Check);

            var generateCommand = new Command("generate", "Generate a model compatible with a Push Dataset")
            {
                new Option<FileInfo>(
                    new string [] { "-model", "-m" },
                    "Model file to convert (.bim)") { IsRequired = true },
                new Option<FileInfo>(
                    new string [] { "-out", "-o" },
                    "Output model ready for Push Datasets (.bim)") { IsRequired = true }
            };
            generateCommand.Handler = CommandHandler.Create<FileInfo, FileInfo>(Generate);

            var publishCommand = new Command("publish", "Publish model as a Push Dataset")
            {
                new Option<FileInfo>(
                    new string [] { "-model", "-m" },
                    "Model file to convert (.bim)") { IsRequired = true },

                new Option<string>(
                    new string [] { "-principal", "-p" },
                    "Service Principal ID") { IsRequired = true },
                new Option<string>(
                    new string [] { "-secret", "-s" },
                    "Secret") { IsRequired = true },
                new Option<string>(
                    new string [] { "-tenant", "-t" },
                    "Tenant ID") { IsRequired = true },
                new Option<string>(
                    new string [] { "-group", "-g" },
                    "Group ID") { IsRequired = true },
                new Option<string>(
                    new string [] { "-datasetname", "-dn" },
                    "Dataset Name") { IsRequired = false },
                new Option<string>(
                    new string [] { "-datasetid", "-ds" },
                    "Dataset ID") { IsRequired = false }
            };
            publishCommand.Handler = CommandHandler.Create<FileInfo, string, string, string, string, string, string>(Publish);

            var alterCommand = new Command("alter", "Alter model published as a Push Dataset")
            {
                new Option<FileInfo>(
                    new string [] { "-model", "-m" },
                    "Model file to convert (.bim)") { IsRequired = true },

                new Option<string>(
                    new string [] { "-principal", "-p" },
                    "Service Principal ID") { IsRequired = true },
                new Option<string>(
                    new string [] { "-secret", "-s" },
                    "Secret") { IsRequired = true },
                new Option<string>(
                    new string [] { "-tenant", "-t" },
                    "Tenant ID") { IsRequired = true },
                new Option<string>(
                    new string [] { "-group", "-g" },
                    "Group ID") { IsRequired = true },
                new Option<string>(
                    new string [] { "-datasetname", "-dn" },
                    "Dataset Name") { IsRequired = false },
                new Option<string>(
                    new string [] { "-datasetid", "-ds" },
                    "Dataset ID") { IsRequired = false }
            };
            alterCommand.Handler = CommandHandler.Create<FileInfo, string, string, string, string, string, string>(Alter);

            var refreshCommand = new Command("refresh", "Refresh a Push Dataset executing specified DAX queries")
            {
                new Option<FileInfo>(
                    new string [] { "-dax", "-x" },
                    "Queries to execute (.dax)"){ IsRequired = true }, 
                new Option<string>(
                    new string [] { "-principal", "-p" },
                    "Service Principal ID") { IsRequired = true },
                new Option<string>(
                    new string [] { "-secret", "-s" },
                    "Secret") { IsRequired = true },
                new Option<string>(
                    new string [] { "-tenant", "-t" },
                    "Tenant ID") { IsRequired = true },
                new Option<string>(
                    new string [] { "-group", "-g" },
                    "Group ID") { IsRequired = true },
                new Option<string>(
                    new string [] { "-datasetname", "-dn" },
                    "Dataset Name") { IsRequired = false },
                //new Option<string>(
                //    new string [] { "-datasetid", "-ds" },
                //    "Dataset ID") { IsRequired = false },
                new Option<string>(
                    new string [] { "-readfromworkspace", "-rw" },
                    "Read from workspace URL") { IsRequired = true },
                new Option<string>(
                    new string [] { "-readfromdatabase", "-rd" },
                    "Read from dataset/database name") { IsRequired = true },
                new Option<string>(
                    new string [] { "-userapp", "-ua" },
                    "Application (client) ID") { IsRequired = false },
                new Option<string>(
                    new string [] { "-username", "-un" },
                    "Username to read dataset") { IsRequired = false },
                new Option<string>(
                    new string [] { "-userpassword", "-up" },
                    "Password to read dataset") { IsRequired = false }
            };
            refreshCommand.Handler = CommandHandler.Create<FileInfo, string, string, string, string, string, string, string, string, string, string>(Refresh);

            var clearCommand = new Command("clear", "Clear all the tables of a Push Dataset")
            {
                new Option<string>(
                    new string [] { "-principal", "-p" },
                    "Service Principal ID") { IsRequired = true },
                new Option<string>(
                    new string [] { "-secret", "-s" },
                    "Secret") { IsRequired = true },
                new Option<string>(
                    new string [] { "-tenant", "-t" },
                    "Tenant ID") { IsRequired = true },
                new Option<string>(
                    new string [] { "-group", "-g" },
                    "Group ID") { IsRequired = true },
                new Option<string>(
                    new string [] { "-datasetname", "-dn" },
                    "Dataset Name") { IsRequired = false },
                new Option<string>(
                    new string [] { "-datasetid", "-ds" },
                    "Dataset ID") { IsRequired = false }
            };
            clearCommand.Handler = CommandHandler.Create<string,string,string,string,string,string>(Clear);

            var simulateCommand = new Command("simulate", "Simulate real-time updates to Push Dataset")
            {
                new Option<FileInfo>(
                    new string [] { "-configuration", "-c" },
                    "Simulation parameters (.json)"){ IsRequired = true },

                new Option<string>(
                    new string [] { "-principal", "-p" },
                    "Service Principal ID") { IsRequired = true },
                new Option<string>(
                    new string [] { "-secret", "-s" },
                    "Secret") { IsRequired = true },
                new Option<string>(
                    new string [] { "-tenant", "-t" },
                    "Tenant ID") { IsRequired = true },
                new Option<string>(
                    new string [] { "-group", "-g" },
                    "Group ID") { IsRequired = true },
                new Option<string>(
                    new string [] { "-datasetname", "-dn" },
                    "Dataset Name") { IsRequired = false },
                new Option<string>(
                    new string [] { "-datasetid", "-ds" },
                    "Dataset ID") { IsRequired = false }
            };
            simulateCommand.Handler = CommandHandler.Create<FileInfo, string, string, string, string, string, string>(Simulate);


            // Create a root command with some options
            rootCommand = new RootCommand
            {
                checkCommand,
                generateCommand,
                publishCommand,
                alterCommand,
                refreshCommand,
                clearCommand,
                simulateCommand
            };
            rootCommand.Description = $"Power BI Push Dataset Tools v.{typeof(CommandLine).Assembly.GetName().Version}";
            
            // Parse the incoming args and invoke the handler
            return rootCommand.InvokeAsync(args).Result;
        }
    }
}