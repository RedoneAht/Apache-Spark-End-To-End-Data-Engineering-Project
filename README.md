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
*   **Performance Optimizations:**
    *   **Broadcast Joins:** Broadcasts smaller datasets (e.g., customer dimension tables) to all worker nodes to minimize expensive network shuffles, converting wide transformations into narrow transformations. The project also implements the advanced strategy of **repartitioning the larger DataFrame before broadcasting** to further optimize local join performance.
    *   **Partitioning & Bucketing:** Loads data into the Data Lake with partition folders (e.g., `location=Orlando`) to optimize downstream queries and avoid full table scans. Bucketing is discussed for high-cardinality columns.
    *   **Predicate Pushdown & Columnar Storage:** Leverages columnar formats like Parquet to push filters down to the storage level and select only required columns (predicate pruning), significantly reducing disk I/O.

## Implemented Business Workflows (Transformations)
The system uses a `WorkflowRunner` to schedule and execute distinct analytical pipelines.

1.  **Workflow 1: Customers buying AirPods *immediately after* an iPhone**
    *   **Goal:** Identify customer profiles who purchased an iPhone and subsequently purchased AirPods in their next transaction.
    *   **Implementation:** Utilizes Spark Window functions to `PartitionBy` the customer ID and `orderBy` the transaction date ascending. It applies the **`lead()` function** to peek at the succeeding row and create a `next_product_name` column to filter the exact purchase sequence.
2.  **Workflow 2: Customers exclusively buying iPhones and AirPods**
    *   **Goal:** Isolate customers whose entire purchase history consists *only* of iPhones and AirPods (no other items like MacBooks).
    *   **Implementation:** Groups the transactions by customer ID and applies the **`collect_set()` aggregation function** to generate a distinct array of purchased products. It then filters the records ensuring the array contains both products and strictly has a `size()` of exactly two.

## Future Transformations (Roadmap)
The modular architecture easily supports the addition of new workflows. Planned implementations include:
*   Determining the average time delay between a customer purchasing an iPhone and an AirPod.
*   Identifying the top three selling products in each category based on total revenue.

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
