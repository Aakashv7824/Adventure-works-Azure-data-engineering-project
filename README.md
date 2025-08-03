# Silver-Layer ETL & Analysis: AdventureWorks Dataset

This notebook implements a **Silver-layer** ETL pipeline for the AdventureWorks sample data, using PySpark on Azure Data Lake Storage (ADLS Gen2). It reads raw CSV files from the **Bronze** zone, applies business-level transformations to produce clean Parquet tables in the **Silver** zone, and concludes with a basic sales analysis.

---

## 📂 Notebook Structure

1. **Setup & Configuration**  
   - Imports PySpark SQL functions.  
   - Configures ADLS Gen2 OAuth credentials via `spark.conf.set()` for secure access.

2. **Data Access**  
   - Reads seven raw CSV datasets from ADLS:  
     - `AdventureWorks_Calendar.csv`  
     - `AdventureWorks_Customers.csv`  
     - `AdventureWorks_ProductCategories.csv`  
     - `AdventureWorks_Products.csv`  
     - `AdventureWorks_Returns.csv`  
     - `AdventureWorks_Territories.csv`  
     - `AdventureWorks_Sales.csv`  
   - Uses `.option("header", True)` and `.option("inferSchema", True)` for schema inference.

3. **Transformations**  
   - **Calendar**  
     - Extracts `Year`, `Month`, `Quarter` from the date field.  
   - **Customers**  
     - Concatenates name fields into `fullName`, standardizes string casing, and drops nulls.  
   - **Product Categories & Products**  
     - Renames and casts columns (e.g., `ProductCategoryID`, `ListPrice`), drops duplicates.  
   - **Returns**  
     - Parses return dates, reasons, and casts numeric flags.  
   - **Territories**  
     - Trims whitespace from region codes, filters out empty records.  
   - **Sales**  
     - Calculates `LineTotal = UnitPrice * OrderQuantity`, adds date keys for joins.

4. **Silver-Layer Writes**  
   - Persists each transformed DataFrame as Parquet under the ADLS path:  
     ```
     abfss://<container>@<account>.dfs.core.windows.net/silver/<table>/
     ```  
   - Uses `write.format("parquet").mode("append")` for incremental loads.

5. **Sales Analysis**  
   - Runs a simple aggregation on the Silver `Sales` table:  
     ```python
     df_sales
       .groupBy("OrderDate")
       .agg(count("OrderNumber").alias("total_orders"))
       .display()
     ```  
   - Provides a daily order-count trend for exploratory insight.

---

## 🔧 Prerequisites

- **Apache Spark 3.x** with PySpark  
- **Access to ADLS Gen2** with a Service Principal (Client ID / Secret / Tenant)  
- Python packages:  
  ```bash
  pip install pyspark
