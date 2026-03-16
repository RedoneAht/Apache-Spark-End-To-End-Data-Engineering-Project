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
