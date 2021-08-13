using Newtonsoft.Json;
using Newtonsoft.Json.Serialization;
using Newtonsoft.Json.Converters;
using System;
using System.Collections.Generic;

namespace Sqlbi.PbiPushDataset
{
    [JsonConverter(typeof(StringEnumConverter))]
    public enum SimulationType
    {
        /// <summary>
        /// Always write the same value
        /// </summary>
        Fixed,

        /// <summary>
        /// Write a random element from a list of values
        /// </summary>
        List,

        /// <summary>
        /// Choose a random value in the provided range and granularity
        /// </summary>
        Range
    }

    public class SimulationRange
    {
        /// <summary>
        /// Minimum value in the range
        /// </summary>
        public double Min { get; set; }

        /// <summary>
        /// Maximum value in the range
        /// </summary>
        public double Max { get; set; }

        /// <summary>
        /// Range granularity: use 0 for integers, 1 = 0.1, 2 = 0.01, -2 = multiple of 100
        /// </summary>
        public int Granularity { get; set; }
    }

    [JsonObject(ItemNullValueHandling = NullValueHandling.Ignore)]
    public class ColumnParameters
    {
        /// <summary>
        /// Column name
        /// </summary>
        public string Name { get; set; }

        public SimulationType Type { get; set; }

        public SimulationRange Range { get; set; }

        public object[] AllowedValues { get; set; }

        public object FixedValue { get; set; }

        private readonly Random rnd = new Random();

        public object GenerateValue()
        {
            return Type switch
            {
                SimulationType.Fixed => FixedValue,
                SimulationType.List => AllowedValues[rnd.Next(0, AllowedValues.Length)],
                SimulationType.Range => 
                    (Range.Granularity == 0) 
                        ? rnd.Next((int)Range.Min,(int)Range.Max)
                        : (Range.Granularity > 0)
                            ? Math.Round(Range.Min + (rnd.NextDouble() * (Range.Min - Range.Max)), Range.Granularity)
                            : (int)(Math.Pow(10, -Range.Granularity) * (Range.Min + (rnd.NextDouble() * (Range.Min - Range.Max))) ),
                _ => throw new ArgumentException("Simulation Type not defined."),
            };
        }
    }

    public class TableParameters
    {
        /// <summary>
        /// Table name
        /// </summary>
        public string Name { get; set; }

        /// <summary>
        /// Number of rows in each batch
        /// </summary>
        public long BatchRows { get; set; }

        public ColumnParameters[] Columns { get; set; }

        public System.Dynamic.ExpandoObject GenerateRow()
        {
            var row = new System.Dynamic.ExpandoObject();
            foreach (var column in Columns)
            {
                row.TryAdd(column.Name, column.GenerateValue());
            }
            return row;
        }
    }

    public class SimulationParameters
    {
        /// <summary>
        /// Interval between two batches (seconds)
        /// </summary>
        public long BatchInterval { get; set; }

        public TableParameters[] Tables {get; set;}

    }

    public class Simulator
    {
        public SimulationParameters Parameters { get; set; }

        public static Simulator ReadParameters(string path)
        {
            string s = System.IO.File.ReadAllText(path);
            Simulator simulator = new Simulator
            {
                Parameters = JsonConvert.DeserializeObject<SimulationParameters>(s)
            };
            return simulator;
        }

        public void WriteParameters( string path )
        {
            string s = JsonConvert.SerializeObject(Parameters, new JsonSerializerSettings { ContractResolver = new CamelCasePropertyNamesContractResolver() });
            System.IO.File.WriteAllText(path, s);
        }
    }
}
