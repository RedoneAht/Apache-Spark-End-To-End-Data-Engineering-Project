# Apache Spark End-To-End Data Engineering Project | Apple Data Analysis

## Project Overview
This project implements a **full ETL (Extract, Transform, Load) pipeline using Apache Spark (PySpark) and Databricks**. The pipeline processes e-commerce transaction data related to Apple products (such as iPhones, AirPods, and MacBooks) to extract valuable business insights. The codebase is engineered with **production-ready Low-Level Design (LLD) principles**, specifically utilizing the **Factory Method pattern** to create a highly modular and scalable architecture.

## Architecture & Technology Stack
*   **Core Processing Engine:** Apache Spark (PySpark) and Spark SQL.
*   **Execution Environment:** Databricks Community Edition (with Databricks File System - DBFS enabled).
*   **Data Sources (Extract):** Dynamic ingestion from CSV, Parquet, and Delta Tables.
*   **Data Sinks (Load):**
    *   **Data Lake:** Writes data to DBFS in columnar formats (like Parquet) utilizing directory partitioning.
    *   **Lakehouse:** Writes to Databricks Delta Tables, enabling **ACID transaction support and efficient upserts (update + insert)**.

## Key Features & Design Patterns
*   **Factory Design Pattern:** Implements `ReaderFactory` and `LoaderFactory` classes using an abstract interface. This allows the pipeline to dynamically instantiate the correct reader or writer class based on the input data type (CSV, Parquet, or Delta) without modifying core logic.
*   **Modular Codebase:** The pipeline logic is cleanly decoupled into specific `Extractor`, `Transformer`, and `Loader` abstract classes and sub-classes.

## Project Structure
*   `workflow_runner`: Orchestrates the execution of different analytical pipelines (e.g., `first_workflow`, `second_workflow`).
*   `reader_factory` / `loader_factory`: Contains abstract logic and routing for reading/writing multiple data formats.
*   `extractor`: Standardized classes to pull raw transaction and customer records into PySpark DataFrames.
*   `transformer`: Contains the PySpark business logic, window functions, and broadcast joins.
*   `loader`: Handles writing DataFrames to output sinks, including configuring partitions and Delta Table saves.

## Setup & Execution
1.  **Environment Setup:** Create a Databricks Community Edition account and spin up a compute cluster (e.g., Spark 3.3.2, Scala 2.12).
2.  **Enable DBFS:** Go to Admin Settings -> Advanced, and turn on the Databricks File System (DBFS) feature.
3.  **Upload Source Data:** Upload the provided sample data (`customer_updated.csv`, `product_updated.csv`, `transactions_updated.csv`) to DBFS via the Data/Catalog UI.
4.  **Register Source Tables:** Execute the initial notebook commands to register the CSV files as a base `customer_delta_table`.
5.  **Run Pipelines:** Execute the main `Apple analysis` notebook to trigger the workflow runner, applying the extraction, transformation, and loading processes. Check the output directories to view the partitioned Parquet files and Delta tables.
