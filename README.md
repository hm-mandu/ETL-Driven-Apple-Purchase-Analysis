# ETL-Driven-Apple-Purchase-Analysis

This project builds an ETL (Extract, Transform, Load) pipeline using Apache Spark on Databricks to analyze Apple product purchase patterns. It helps identify key customer behaviors, such as customers who bought AirPods after iPhones or those who only purchased iPhones and AirPods.

## Key Features
- Modular ETL Components: Extract, Transform, and Load data efficiently.

- Multiple Data Source Support: Handles CSV, Parquet, and Delta Lake formats.

- Advanced Data Transformation: Uses Spark window functions and array operations to analyze customer purchasing patterns.

- Scalable and Flexible: Can scale for large datasets with optimized partitioning strategies for fast queries.

## How It Works
1. Extract:
   Data is read from different sources (CSV, Parquet, or Delta tables) using the Factory Design Pattern for extensibility.

2. Transform:

- AirPods after iPhone: Identifies customers who bought AirPods immediately after iPhones using window functions and lead().

- Only iPhone and AirPods: Finds customers who only bought iPhones and AirPods using array operations and grouping.

3. Load: Data is loaded into Delta tables or DBFS (Databricks File System) for persistent storage and further analysis.

### Tech Stack
- Apache Spark & PySpark SQL for data transformation and querying.

- Delta Lake for data storage and versioning.

- Databricks for cloud-based data processing.

- GitHub for version control and collaboration.

### Conclusion
This project demonstrates a clean, object-oriented approach to building scalable and maintainable ETL pipelines for analyzing customer purchase patterns using Databricks and Apache Spark. The design patterns and modular components used in this project make it easy to extend and adapt for additional data sources, transformations, and use cases.
