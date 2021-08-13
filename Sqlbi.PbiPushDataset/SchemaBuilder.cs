using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using Microsoft.PowerBI.Api.Models;
using TabModel = Microsoft.AnalysisServices.Tabular;

namespace Sqlbi.PbiPushDataset
{
    /// <summary>
    /// Schema builder.
    /// We assume that measure names are unique and not used on any column reference.
    /// </summary>
    public static class SchemaBuilder
    {
        public static bool CheckModel ( 
            TabModel.Model model,
            out List<TabModel.Table> unsupportedTables,
            out List<TabModel.Measure> unsupportedMeasures,
            out List<TabModel.Relationship> unsupportedRelationships)
        {
            // Check tables
            unsupportedTables = model.Tables.Where(t => t.Name == "Date").ToList(); // TODO: remove the filter of Date table if/when Microsoft fixes the bug on the service
            bool checkedTables = (unsupportedTables.Count == 0);

            // Check measures
            List<TabModel.Measure> allMeasures = model.Tables.SelectMany(t => t.Measures).Select(m => m).ToList();
            bool checkedMeasures = CheckUnsupportedMeasures(allMeasures, out unsupportedMeasures, out List<TabModel.Measure> supportedMeasures);

            // Check relationships
            unsupportedRelationships = model.Relationships.Where(r =>
            {
                // Inactive relationships are not supported
                if (!r.IsActive) return true;

                // 1:1 and M:M cardinality relationships are not supported
                TabModel.SingleColumnRelationship sr = r as TabModel.SingleColumnRelationship;
                return (sr?.FromCardinality == sr?.ToCardinality);
            }).ToList();
            bool checkedRelationships = (unsupportedRelationships.Count == 0);

            return (checkedMeasures && checkedRelationships);
        }

        /// <summary>
        /// Find supported measures in the list of all the measures of the data model.
        /// Measures with USERELATIONSHIP or with any direct or indirect reference 
        /// to measures with USERELATIONSHIP are not supported.
        /// </summary>
        /// <param name="allMeasures">List of all the measures to analyze for dependencies</param>
        /// <param name="unsupportedMeasures">List of unsupported measures</param>
        /// <param name="supportedMeasures">List of supported measures</param>
        /// <returns></returns>
        public static bool CheckUnsupportedMeasures(
            List<TabModel.Measure> allMeasures,
            out List<TabModel.Measure> unsupportedMeasures,
            out List<TabModel.Measure> supportedMeasures)
        {
            unsupportedMeasures = new List<TabModel.Measure>();
            bool foundUnsupportedMeasure;
            do
            {
                supportedMeasures = new List<TabModel.Measure>();
                foundUnsupportedMeasure = false;
                foreach (var m in allMeasures)
                {
                    bool referenceToUnsupportedMeasure =
                        unsupportedMeasures.FirstOrDefault(um => m.Expression.Contains($"[{um.Name}]", StringComparison.OrdinalIgnoreCase)) != null;

                    if (referenceToUnsupportedMeasure || m.Expression.Contains("USERELATIONSHIP", StringComparison.OrdinalIgnoreCase))
                    {
                        unsupportedMeasures.Add(m);
                        foundUnsupportedMeasure = true;
                    }
                    else
                    {
                        supportedMeasures.Add(m);
                    }
                }
                allMeasures = supportedMeasures;
            } while (foundUnsupportedMeasure);
            return unsupportedMeasures.Count == 0;
        }

        public static CreateDatasetRequest GetDatasetRequest(
            string datasetName,
            TabModel.Model model,
            Action<TabModel.Measure> unsupportedMeasureAction,
            Action<TabModel.Relationship> unsupportedRelationshipAction
        )
        {
            return GetDataset(
                datasetName,
                model.Tables,
                model.Relationships,
                unsupportedMeasureAction,
                unsupportedRelationshipAction
                );
        }

        public static CreateDatasetRequest GetDataset(
            string datasetName, 
            TabModel.TableCollection tables, 
            TabModel.RelationshipCollection relationships,
            Action<TabModel.Measure> unsupportedMeasureAction,
            Action<TabModel.Relationship> unsupportedRelationshipAction
            )
        {
            List<TabModel.Measure> allMeasures = tables.SelectMany(t => t.Measures).Select(m => m).ToList();
            if (!CheckUnsupportedMeasures(allMeasures, out List<TabModel.Measure> unsupportedMeasures, out List<TabModel.Measure> supportedMeasures))
            {
                // Diagnostic - list of unsupported measures
                foreach (var m in unsupportedMeasures)
                {
                    unsupportedMeasureAction?.Invoke(m);
                }
            }

            var datasetSchema = new CreateDatasetRequest
            {
                Name = datasetName,
                Tables = new List<Table>(),
                Relationships = new List<Relationship>()
            };

            foreach (var t in tables)
            {
                datasetSchema.Tables.Add(GetTable(t, unsupportedMeasures));
            }

            foreach (var r in relationships)
            {
                bool addedRelationship = false;
                if ((r is TabModel.SingleColumnRelationship sr) && sr.IsActive && (sr.FromCardinality != sr.ToCardinality))
                {
                    if (r != null)
                    {
                        var pushRelationship = new Relationship
                        {
                            Name = sr.Name,
                            FromTable = sr.FromTable.Name,
                            FromColumn = sr.FromColumn.Name,
                            ToTable = sr.ToTable.Name,
                            ToColumn = sr.ToColumn.Name,
                            CrossFilteringBehavior =
                                r.CrossFilteringBehavior == TabModel.CrossFilteringBehavior.OneDirection ? CrossFilteringBehavior.OneDirection :
                                r.CrossFilteringBehavior == TabModel.CrossFilteringBehavior.BothDirections ? CrossFilteringBehavior.BothDirections :
                                r.CrossFilteringBehavior == TabModel.CrossFilteringBehavior.Automatic ? CrossFilteringBehavior.Automatic : null
                        };
                        datasetSchema.Relationships.Add(pushRelationship);
                        addedRelationship = true;
                    }
                }
                if (!addedRelationship)
                {
                    unsupportedRelationshipAction?.Invoke(r);
                }
            }

            return datasetSchema;
        }

        /// <summary>
        /// Gets the table schema.
        /// </summary>
        /// <returns>The table.</returns>
        /// <param name="type">Type.</param>
        public static Table GetTable(Type type)
        {
            var table = new Table()
            {
                Name = type.Name,
                Columns = new List<Column>()
            };

            var properties = type.GetProperties().Where(x => x.CanRead && x.CanWrite).ToList();

            foreach (var propertyInfo in properties)
                table.Columns.Add(GetColumn(propertyInfo));

            return table;
        }

        public static Table GetTable(TabModel.Table t, List<TabModel.Measure> unsupportedMeasures)
        {
            var table = new Table()
            {
                Name = t.Name,
                Columns = new List<Column>(),
                Measures = new List<Measure>()
            };

            foreach (var c in t.Columns)
            {
                table.Columns.Add(GetColumn(c));
            }

            foreach (var m in t.Measures)
            {
                if (!unsupportedMeasures.Contains(m))
                {
                    table.Measures.Add(
                        new Measure
                        {
                            Name = m.Name,
                            Expression = m.Expression,
                            IsHidden = m.IsHidden,
                            FormatString = m.FormatString,
                            Description = m.Description
                        }
                    );
                }
            }

            return table;
        }

        /// <summary>
        /// Gets the column schema.
        /// </summary>
        /// <returns>The column.</returns>
        /// <param name="propertyInfo">Property info.</param>
        static Column GetColumn(TabModel.Column c)
        {
            var column = new Column
            {
                Name = c.Name,
                DataType = GetDataType(c.DataType),
                DataCategory = c.DataCategory,
                FormatString = c.FormatString,
                IsHidden = c.IsHidden,
                SortByColumn = c.SortByColumn?.Name,
                SummarizeBy = c.SummarizeBy.ToString()
            };

            return column;
        }


        /// <summary>
        /// Gets the column schema.
        /// </summary>
        /// <returns>The column.</returns>
        /// <param name="propertyInfo">Property info.</param>
        static Column GetColumn(PropertyInfo propertyInfo)
        {
            var column = new Column
            {
                Name = typeof(Nullable<>).IsAssignableFrom(propertyInfo.PropertyType)
                    ? propertyInfo.PropertyType.GenericTypeArguments[0].Name
                    : propertyInfo.Name,
                DataType = GetDataType(propertyInfo.PropertyType)
            };

            return column;
        }

        /// <summary>
        /// Gets the type of the data.
        /// </summary>
        /// <returns>The data type.</returns>
        /// <param name="type">Type.</param>
        static string GetDataType(Type type)
        {
            string dataType = type.Name switch
            {
                "Int32" => "Int64",
                "Int64" => "Int64",
                "Double" => "Double",
                "Boolean" => "bool",
                "DateTime" => "DateTime",
                "String" => "string",
                _ => throw new ArgumentException(string.Format("Invalid argument. Type {0} isn't support by the PowerBI WebApi", type.FullName)),
            };
            return dataType;
        }

        /// <summary>
        /// Gets the type of the data.
        /// </summary>
        /// <returns>The data type.</returns>
        /// <param name="type">Type.</param>
        static string GetDataType(TabModel.DataType type)
        {
            string dataType = type switch
            {
                TabModel.DataType.Decimal => type.ToString(),
                TabModel.DataType.Int64 => type.ToString(),
                TabModel.DataType.Double => type.ToString(),
                TabModel.DataType.DateTime => type.ToString(),
                TabModel.DataType.Boolean => type.ToString(),
                TabModel.DataType.String => type.ToString(),

                _ => throw new ArgumentException(string.Format("Invalid argument. Type {0} isn't support by the PowerBI WebApi", type.ToString())),
            };
            return dataType;
        }
    }
}
