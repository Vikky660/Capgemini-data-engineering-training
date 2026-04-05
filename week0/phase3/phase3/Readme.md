## Phase 3 – PySpark ETL Pipeline

### Objective  
This phase focuses on building an end-to-end ETL pipeline using PySpark to process data efficiently and extract meaningful insights.

### Problem Overview  
In this project, customer and sales datasets were used in multiple file formats such as CSV, JSON, and Parquet.

The main tasks included:
- Loading data from different file formats  
- Cleaning and validating the data  
- Performing transformations and aggregations  
- Generating insights such as daily sales and city-wise revenue  
- Identifying repeat customers and top-performing customers  

### Approach  
The following steps were followed during implementation:
- Loaded datasets using PySpark DataFrames  
- Inspected data using `show()` and `printSchema()`  
- Cleaned data by removing null and invalid records  
- Handled multiple data formats (CSV, JSON, Parquet)  
- Applied transformations including joins and aggregations  
- Used window functions to rank customers  

### Key Transformations  
- `groupBy()` and `agg()` → used for aggregations  
- `join()` → used to combine datasets  
- `filter()` and `dropna()` → used for data cleaning  
- `withColumn()` and conditional logic → used for transformations  
- Window functions → used for ranking and advanced analysis  

### Results  
The pipeline generated the following outputs:
- Daily sales summary  
- Revenue generated per city  
- List of repeat customers  
- Top customer in each city  
- Final aggregated customer report  

All outputs are stored in the `OUTPUTS/` folder.

### Challenges Faced  
- Handling missing and inconsistent data  
- Writing and understanding window functions  
- Managing and processing multiple file formats  
- Applying correct transformation logic  

### Learnings  
- Gained experience in building end-to-end ETL pipelines using PySpark  
- Improved data cleaning and validation techniques  
- Learned to work with multiple data formats efficiently  
- Developed a better understanding of window functions and analytics  

### Project Structure  
- `solution/` → Contains PySpark ETL implementation  
- `phase3_problem_statement/` → Problem description  
- `OUTPUTS/` → Output screenshots  
- `README.md` → Project documentation  
