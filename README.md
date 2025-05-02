# Incremental Monitoring Data Access Pipeline in Microsoft Fabric

## Introduction
This repository contains a solution for accessing incremental data from the Monitoring database in Microsoft Fabric. The Monitoring feature in Fabric allows users to gain insights into workspace performance by logging activity data into a read-only KQL database within the Monitoring Eventhouse. However, accessing this data programmatically via service principals, Python code, notebooks, shortcuts, or OneLake is currently restricted. This project implements a pipeline to incrementally extract and store Monitoring database data for further analysis.

## Problem Statement
The Monitoring database in Microsoft Fabric collects workspace activity data, which can be queried directly within the Monitoring Eventhouse. However, there are limitations:
- Data cannot be accessed programmatically using service principals, Python code in notebooks, shortcuts, or OneLake.
- There is a need to extract incremental data (e.g., data from the last 5 minutes) to enable real-time or near-real-time analysis.
- The solution must handle multiple data streams (e.g., EventhouseCommandLogs, EventhouseQueryLogs) and store them in a structured format for downstream use.

The goal is to create a scalable and automated pipeline to extract incremental data from the Monitoring database and make it accessible for analysis.

## Solution
The proposed solution uses a Microsoft Fabric pipeline (`PL LoggingDetails`) to incrementally extract data from the Monitoring KQL database and store it in a target KQL database. The pipeline includes:
- A **ForEach activity** to process multiple data streams (e.g., EventhouseCommandLogs, EventhouseQueryLogs) in parallel.
- A **Copy activity** to transfer data from the source KQL database to a target KQL database.
- A **Notebook activity** to log pipeline execution details (e.g., pipeline name, run ID, rows read/written, status) into a Lakehouse table and CSV files for auditing and analysis.

The pipeline is parameterized to handle different data streams and queries, ensuring flexibility and scalability. The notebook processes and logs metadata about each pipeline run, enabling monitoring and debugging.

## Steps to Create the Pipeline
1. **Enable Monitoring in Workspace Settings**:
   - Navigate to the Microsoft Fabric workspace.
   - Go to **Workspace Settings** > **Monitoring**.
   - Turn on **Log workspace activity** to enable data collection in the Monitoring KQL database.
   - Note the link to the Monitoring KQL database within the Monitoring Eventhouse.

2. **Create the Pipeline**:
   - In Microsoft Fabric, create a new pipeline named `PL LoggingDetails`.
   - Add a **ForEach activity** (`ForEachArray`) to iterate over an array of data streams.
   - Define the `arrayfile` parameter with a JSON array specifying source queries and target tables (e.g., `EventhouseCommandLogs`, `EventhouseQueryLogs`).
   - Inside the ForEach activity, add:
     - A **Copy activity** (`CopydataKQLtoKQL`) to copy data from the source KQL database to the target KQL database.
     - A **Notebook activity** (`LogNotebook`) to log execution details.

3. **Configure the Copy Activity**:
   - Set the source to the Monitoring KQL database using a `KustoDatabaseSource`.
   - Specify the query (e.g., `EventhouseCommandLogs | where Timestamp >= ago(5m)`) to extract incremental data.
   - Set the sink to the target KQL database using a `KustoDatabaseSink`.
   - Configure the linked services for both source and target databases with appropriate workspace IDs, endpoints, and database IDs.

4. **Configure the Notebook Activity**:
   - Create a notebook in Fabric to process pipeline metadata.
   - Link the notebook to the pipeline using the `notebookId` and `workspaceId`.
   - Pass parameters (e.g., `PipelineName`, `RunId`, `RowsRead`, `RowsWritten`) from the Copy activity to the notebook.

5. **Schedule the Pipeline**:
   - Set up a schedule (e.g., every 5 minutes) to run the pipeline and extract incremental data.
   - Monitor pipeline runs using Fabric’s monitoring tools.

6. **Test and Validate**:
   - Run the pipeline and verify that data is copied to the target KQL database.
   - Check the Lakehouse table (`ActivityDetailLogs`) and CSV files for logged metadata.
   - Query the target KQL database to ensure data integrity.

## Explanation of Code in Notebook Activity
The notebook (`LogNotebook`) processes metadata from the pipeline’s Copy activity and logs it for auditing and analysis. Below is a detailed explanation of the code:

- **Initialization**:
  - A Spark session is initialized to process data using PySpark.
  - Logging is set up to capture information, warnings, and errors.

- **Parameter Retrieval**:
  - The `get_parameter` function retrieves pipeline parameters (e.g., `PipelineName`, `RunId`, `RowsRead`) using multiple methods:
    - Global variables (injected by Fabric).
    - Spark configurations (`spark.conf.get`).
    - Databricks utilities (`dbutils.widgets.get`, if available).
    - A default value if all methods fail.
  - This ensures robustness in parameter retrieval across different Fabric environments.

- **Parameter Processing**:
  - Parameters like `ExecutionDuration`, `RowsRead`, and `RowsWritten` are converted to integers, with error handling for invalid values.
  - All parameters are logged for debugging.

- **DataFrame Creation**:
  - A schema is defined for the DataFrame, including fields like `PipelineName`, `RunId`, `ExecutionStatus`, etc.
  - A single-row DataFrame is created with the retrieved parameters.

- **Data Storage**:
  - The DataFrame is saved to a Lakehouse table (`ActivityDetailLogs`) in append mode for persistent storage.
  - The DataFrame is also saved as a CSV file in the `Files/Logs/ActivityLogs` directory, with a unique filename based on `RunId` and `Timestamp`.

- **Output**:
  - A summary is printed to confirm the processing of parameters, including pipeline name, run ID, table, status, rows read/written, and timestamp.

The notebook ensures that pipeline execution details are logged reliably, enabling monitoring and troubleshooting.

## Additional Considerations
- **Security**:
  - Ensure that linked services for the source and target KQL databases are configured with appropriate authentication (e.g., managed identity or service principal).
  - Restrict access to the target KQL database and Lakehouse table to authorized users.

- **Scalability**:
  - The pipeline’s `batchCount` (set to 50) can be adjusted based on the number of data streams and performance requirements.
  - Consider partitioning the target KQL database tables for large datasets.

- **Error Handling**:
  - The pipeline includes a retry policy (1 retry, 30-second interval) for the Copy activity.
  - The notebook logs errors during parameter retrieval and data storage, aiding in debugging.

- **Monitoring and Alerts**:
  - Use Fabric’s monitoring tools to track pipeline runs and detect failures.
  - Set up alerts for pipeline failures or anomalies in rows read/written.

- **Future Enhancements**:
  - Add data validation checks in the notebook to ensure data quality.
  - Implement a mechanism to archive old logs in the Lakehouse table or CSV files.
  - Explore integrating with Power BI for visualizing pipeline performance metrics.

This repository provides a comprehensive solution for accessing incremental Monitoring database data in Microsoft Fabric, with a focus on automation, scalability, and auditability.
