
# Power BI Push Dataset tools
## Description
The Power BI Push Dataset tools is a collection of libraries and tools to create Power BI real-time solutions based on push datasets.
You can find a complete description of the architecture of such a solution at [SQLBI](https://www.sqlbi.com/)

There are three projects included in the repository:
*	**Sqlbi.PbiPushDataset**: it is a library based on .NET Core 3.1, which is compatible with Azure Functions. This library could be used directly by your C# or .NET code if you do not want to rely on PowerShell or command-line tools to create the push dataset model. This library validates and translates regular Tabular models in Push Datasets, also providing helper functions to clear, populate, and simulate real-time transactions. The library is available as a [NuGet package](https://www.nuget.org/packages/Sqlbi.PbiPushDataset/).
*	**PbiPushTools**: it is a command-line tool that internally uses the Sqlbi.PbiPushDataset library. This tool does not require programming skills to create, maintain, and test a push dataset model.
*	**Sqlbi.PbiPushTools**: it is a PowerShell cmdlet to create, maintain, and test a push dataset model. The cmdlet internally uses the Sqlbi.PbiPushDataset library and is an alternative to the command-line tool. This cmdlet does not require programming skills to create, maintain, and test a push dataset model. The PowerShell cmdlet is compatible with PowerShell scripts managed by Azure Functions. The cmdlet is available in the [PowerShell gallery](https://www.powershellgallery.com/packages/Sqlbi.PbiPushTools/).

## Sqlbi.PbiPushDataset
The **[PbiConnection](https://github.com/sql-bi/Pbi-PushDataset/blob/main/Sqlbi.PbiPushDataset/PbiConnection.cs)** class is a wrapper of the push datasets features included in the Power BI REST API. 
The push dataset is created starting from a Tabular Object Model (TOM) structure, removing the unsupported features (like user hierarchies and inactive relationships).
The push dataset can be initialized reading data from another Power BI dataset.
The class handles service principal authentication and user authentication for the refresh operation.
The class also includes a simulator that creates and write rows according to a configuration table.
The public methods available are:
* **CreatePushDataset**: Creates a push dataset in the specified workspace using the TOM model retrieved from a model.bim file.
* **AlterPushDataset**: Updates the structure of a push dataset in the specified workspace using the TOM model retrieved from a model.bim file.
* **ClearPushDataset**: Remove all the rows from one or all the tables of a push dataset
* **RefreshWithDax**: Writes in tables of a push dataset the result obtained by running one or more DAX queries on another dataset published on the same Power BI tenant (also on a different workspace).
* **PushSimulation**: Runs a simulation writing rows in one or more tables of a push dataset at intervals specified by the simulator configuration.

### Simulator configuration
This is a sample of the simulator configuration file.
* **batchInterval** is the number of seconds between each batch of rows generated.
* **batchRows** is the number of rows generated in each batch.
* **type** is the algorithm used to generate values for a column, can be **Fixed**, **List**, or **Range**.
* **fixedValue** is the value assigned to a column for the **Fixed** algorithm
* **allowedValues** is a list of values allowed for the **List** algorithm
* **range** describe the range of valuesfor the **Range** algorithm:
    - **min** is the minimum value
    - **max** is the maximum value
    - **granularity** is the range granularity: use 0 for integers, 1 = 0.1, 2 = 0.01, -2 = multiple of 100
```json
{
    "batchInterval": 2,
    "tables": [
        {
            "name": "Sales",
            "batchRows": 98,
            "columns": [
                {
                    "name": "Quantity",
                    "type": "List",
                    "allowedValues": [1,2,3,4,5,6,7,8,9,10]
                },
                {
                    "name": "Net Price",
                    "type": "Range",
                    "range": {
                        "min": 10.0,
                        "max": 200.0,
                        "granularity": 0
                    }
                },
                {
                    "name": "Unit Price",
                    "type": "Range",
                    "range": {
                        "min": 200.0,
                        "max": 2000.0,
                        "granularity": 0
                    }
                },
                {
                    "name": "Order Date",
                    "type": "Fixed",
                    "fixedValue": "2020-03-05"
                },
                {
                    "name": "StoreKey",
                    "type": "List",
                    "allowedValues": [550,999999,400,210,585,605,230,620,190,465,180,420,290,670,300,80,510,590,610,430,440,450,35,200,540,170,90,650,380,50,240,570,100,220,270,370,560,500,490,360,330,630,140,470,10,255,660,340,310,40,130,530,390,160,410,480,320,260,74,60,150,120,72,280,20,250,350,460,580,520,110,30,70,600]
                }
            ]
        }
    ]
}
```
