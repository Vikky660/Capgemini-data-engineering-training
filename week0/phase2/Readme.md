## Phase 2 – Data Transformations with PySpark

### Objective  
In this phase, the focus is on performing data transformations using PySpark and understanding how SQL concepts like joins and aggregations can be applied to real-world datasets.

### Problem Summary  
In this project, customer and sales datasets were used to analyze data and extract meaningful insights.

The key tasks included:
- Calculating the total amount spent by each customer  
- Identifying the top 3 customers based on spending  
- Finding customers with no orders  
- Generating city-wise revenue  
- Calculating the average order amount  
- Identifying customers with multiple orders  
- Sorting customers based on total spending  

### Approach  
The following steps were followed to complete the analysis:
- Loaded datasets using PySpark  
- Cleaned the data by removing rows with null `customer_id`  
- Applied various transformations:
  - Used `groupBy()` for aggregations  
  - Calculated sum, average, and count  
  - Performed joins between customer and sales datasets  
  - Applied filtering and sorting to derive final results  

### Key Transformations Used  
- `groupBy()` → grouping data  
- `agg()` → calculating sum, average, and count  
- `join()` → combining datasets  
- `filter()` → applying conditions  
- `orderBy()` → sorting data  
- `round()` → handling decimal precision  

### Output / Results  
The following outputs were generated:
- Total amount spent by each customer  
- Top 3 customers based on spending  
- Customers with no orders  
- City-wise total revenue  
- Average order amount per customer  
- Customers with more than one order  
- Customers sorted by total spending  

All outputs are available in the `outputs/` folder.

### Data Engineering Considerations  
- Removed null values to ensure accurate results  
- Maintained consistent column naming across transformations  
- Applied rounding to manage decimal precision  
- Validated results at each step of the process  

### Challenges Faced  
- Confusion between similar column names (`order_amount` vs `total_amount`)  
- Writing correct aggregation logic  
- Handling decimal precision in outputs  
- Understanding joins in PySpark  

### Learnings  
- Gained hands-on experience with PySpark aggregations  
- Improved understanding of joins on real datasets  
- Recognized the importance of data cleaning  
- Strengthened overall PySpark workflow skills  

### Project Structure  
- `pyspark_code/` → PySpark implementation  
- `phase2_problem_statement/` → Problem description  
- `outputs/` → Output screenshots  
- `README.md` → Project documentation  
