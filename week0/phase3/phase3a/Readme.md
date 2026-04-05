## Phase 3A – Data Cleaning with PySpark

### Objective  
This phase focuses on cleaning and preparing raw data using PySpark. The goal is to understand how poor data quality can impact analysis and how to handle such issues effectively.

---

### Problem Overview  
A customer dataset with inconsistencies such as missing values, duplicate records, and invalid entries was used for this task.

The main activities included:
- Detecting null values, duplicates, and incorrect data  
- Cleaning and standardizing the dataset  
- Validating the cleaned data  
- Performing aggregation to analyze customer distribution by city  

---

### Approach  
The following steps were carried out:
- Loaded the dataset into PySpark DataFrames  
- Explored the dataset to identify issues like nulls and duplicates  
- Applied data cleaning techniques:
  - Removed rows with null `customer_id`  
  - Filled missing values with `"Unknown"` where applicable  
  - Eliminated duplicate records  
  - Filtered out invalid age values (age > 0)  
- Verified the cleaning process using row counts before and after  
- Aggregated data using `groupBy()` for analysis  

---

### Key Transformations  
- `dropna()` → removing critical null values  
- `fillna()` → replacing missing entries  
- `dropDuplicates()` → eliminating duplicate records  
- `filter()` → removing invalid data  
- `groupBy()` → grouping data for analysis  
- `count()` → calculating totals per group  

---

### Results  
- Successfully identified and handled null, duplicate, and invalid records  
- Produced a clean and reliable dataset  
- Compared dataset size before and after cleaning  
- Generated customer counts for each city  

---

### Data Engineering Considerations  
- Preserved data integrity by ensuring valid primary keys  
- Handled missing values without excessive data loss  
- Removed duplicates to avoid skewed results  
- Ensured only valid data was used for analysis  

---

### Challenges Faced  
- Detecting multiple types of data quality issues  
- Choosing appropriate strategies for missing values  
- Preventing accidental loss of useful data  
- Understanding the impact of dirty data on results  

---

### Learnings  
- Data cleaning is a critical step in any data pipeline  
- Real-world datasets are often messy and inconsistent  
- Proper validation improves trust in results  
- Clean data leads to more accurate insights  

---

### Project Structure  
- `pyspark_code/` → PySpark data cleaning scripts  
- `outputs/` → Execution screenshots  
- `phase3a_problem_statement/` → Problem description  
- `README.md` → Project documentation  
