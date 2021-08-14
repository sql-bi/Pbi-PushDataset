
# Power BI Push Dataset tools
## Description
The Power BI Push Dataset tools is a collection of libraries and tools to create Power BI real-time solutions based on push datasets.
You can find a complete description of the architecture of such a solution at [SQLBI](https://www.sqlbi.com/)

There are three projects included in the repository:
*	**Sqlbi.PbiPushDataset**: it is a library based on .NET Core 3.1, which is compatible with Azure Functions. This library could be used directly by your C# or .NET code if you do not want to rely on PowerShell or command-line tools to create the push dataset model. This library validates and translates regular Tabular models in Push Datasets, also providing helper functions to clear, populate, and simulate real-time transactions.
*	**PbiPushTools**: it is a command-line tool that internally uses the Sqlbi.PbiPushDataset library. This tool does not require programming skills to create, maintain, and test a push dataset model.
*	**Sqlbi.PbiPushTools**: it is a PowerShell cmdlet to create, maintain, and test a push dataset model. The cmdlet internally uses the Sqlbi.PbiPushDataset library and is an alternative to the command-line tool. This cmdlet does not require programming skills to create, maintain, and test a push dataset model. The PowerShell cmdlet is compatible with PowerShell scripts managed by Azure Functions.

