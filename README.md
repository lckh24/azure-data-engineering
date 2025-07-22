# Azure End-to-End Data Engineering Real-Time Project  
This project simulates a data engineering pipeline for a fictional business case, created as a learning tool to deepen my understanding of data workflows.  
## Problem statement and Business Request  
Agricultural sector plays a critical role in the nation's economy and food security. However, the lack of timely, accessible, and actionable insights into daily market prices of agricultural products creates significant challenges for farmers, policymakers, and supply chain stakeholders. Without a clear understanding of pricing trends, regional market dynamics, and product-level fluctuations, it becomes difficult to make informed decisions related to harvesting, distribution, procurement, and policy planning.  
## Project Overview   
To address this, we suggest a cloud-based data engineering pipeline that automates the collection, processing, and storage of market price data. This ensures that clean, ready-to-use data is available daily to support downstream use cases such as machine learning forecasting models and business analytics, enabling stakeholders to plan, operate, and respond more effectively to market dynamics.   
## Technologies Used  
- **Azure Databricks**: Used for large-scale data processing and transformation through distributed Spark jobs.

- **Azure Data Lake Storage (ADLS)**: Serves as the central storage layer for raw, cleaned, and curated data.

- **Power BI**: Utilized for building interactive dashboards and generating insightful business reports.

- **Azure Key Vault**: Ensures secure storage and access to secrets, credentials, and keys.

- **Azure Entra ID**: Provides centralized identity and access management across Azure services.

- **Azure Monitor & Logging**: Enables real-time monitoring, diagnostics, and logging of pipeline performance and system activities.   
## Data Architecture  
<img width="902" height="549" alt="image" src="https://github.com/user-attachments/assets/3eae63c1-0785-4825-9dc0-9e046506dfc4" />  

This solution implements a Lakehouse architecture on Azure Databricks, combining the reliability of a data warehouse with the flexibility and scalability of a data lake.  

## Data Ingestion
Data is ingested daily from multiple sources, including **CSV files**, **REST APIs**, and **SQL-based databases**.  
Ingestion is orchestrated using **Apache Spark batch jobs** in **Databricks**, enabling **scalable** and **reliable** processing.  
Support for **Change Data Capture (CDC)** allows the system to ingest only **new or updated records**, minimizing redundant processing and improving efficiency.

## Storage & Processing
All ingested data is stored in **Azure Data Lake Storage Gen2**, following a **Delta Lake multi-layer architecture**:

- **Bronze Layer**: Raw, unfiltered ingested data  
- **Silver Layer**: Cleaned, deduplicated, and joined datasets  
- **Gold Layer**: Aggregated, business-ready data for analytics  

**Auto Loader** is used for schema inference and supports **incremental data loading** between layers.  
Data **transformation** and **enrichment** are performed via **scheduled batch jobs**, ensuring consistency, reliability, and optimal performance.  

## Consumption
The **Gold-layer** datasets serve as a foundation for various downstream consumers:

- **Power BI**: Interactive dashboards and business reports  
- **Azure Synapse Analytics / Azure Data Explorer**: Advanced querying and exploratory analysis  
- **Azure Machine Learning**: Feature extraction and model training for predictive analytics

## Set Up Project  
## Prerequisites:
* An Azure account with sufficient credits.
## Step 1: Azure Environment Setup

1. **Create Resource Group**  
   Set up a new Azure Resource Group that includes the necessary services such as:
   - Azure Databricks
   - Azure Data Lake Storage Gen2

   ![Create Resource Group](https://github.com/user-attachments/assets/f0d6a7eb-a224-4492-a512-046c97b099be)

2. **Set Up ADLS Containers**  
   In Azure Data Lake Storage, create the following container hierarchy:
   - `bronze`: for raw ingested data  
   - `silver`: for cleaned and transformed data  
   - `gold`: for business-ready, aggregated data  
   - `pricing_analytics`: for processing of unity catalog in databrick    

   ![ADLS Containers](https://github.com/user-attachments/assets/a9e51314-1022-48cf-be48-ade68abed45d)

3. **Configure Access Permissions**  
   - Create a user/service principal with full access to Databricks and Unity Catalog.
   - Grant necessary roles

4. **Set Up Azure Key Vault**  
   - Use Key Vault to securely manage secrets

## Step 2: Data Ingestion

1. **Set Up Databricks Workspace**  
   - Create and configure a Databricks workspace for development and job scheduling.

2. **Configure Compute Cluster**  
   - Launch a cluster with appropriate configuration (e.g., `Standard_DS5_v2`) for ingestion workloads.  
   <img width="1851" height="894" alt="image" src="https://github.com/user-attachments/assets/d8608fe0-8d20-4791-8179-c7f9ee8af916" />


3. **Ingest Source Data to Delta Tables**  
   - Implement ingestion logic (e.g., using PySpark or SQL) to read data from source systems (e.g., CSV, Parquet, APIs).
   - Write data into **Delta Lake** tables under the **bronze** layer.

4. **Automate with Scheduled Jobs**  
   - Set up a **daily batch job** in Databricks to automate the ingestion process.
   - Use `dbutils.notebook.run()` or workflow tasks to organize the pipeline.
   <img width="1843" height="879" alt="image" src="https://github.com/user-attachments/assets/d417d95c-d036-4f49-8343-22dbf846caeb" />


5. **Unity Catalog Integration**  
   - Ensure all Delta tables are created and managed under **Unity Catalog**, specifically within the `pricing_analytics` schema for proper governance and access control.  
  <img width="1844" height="870" alt="image" src="https://github.com/user-attachments/assets/42df1cb8-47cb-440f-8c6a-688240f75e06" />


