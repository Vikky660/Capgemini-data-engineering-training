# 📊 Week 1 – Day 4: Student Submission Analysis (Advanced SQL + PySpark)

## 🚀 Objective
Analyze student submission data by handling real-world data challenges such as:
- Duplicate records
- Invalid entries
- Multiple email mappings

The goal is to build a robust data pipeline using SQL and PySpark.

---

## 🧩 Problem Overview

This project works with three datasets:

1. **Student Master Data**
2. **Task 1 Responses**
3. **Task 1 File 2** (contains duplicates & invalid data)

### 🎯 Goals:
- Clean and standardize raw data  
- Map multiple emails to a single student  
- Identify:
  - ✅ Valid submissions  
  - ❌ Invalid submissions  
  - ⚠️ Duplicate submissions  
  - 🚫 Missing submissions  

---

## 🛠️ Tech Stack

- PySpark  
- SQL (Spark SQL)  
- Databricks / Jupyter Notebook  

---

## 🔄 Data Processing Pipeline

### 1️⃣ Data Ingestion
- Loaded CSV files into Spark DataFrames  
- Created temporary views for SQL analysis  

---

### 2️⃣ Data Cleaning

#### ✔ Column Standardization
- Removed spaces and special characters  
- Renamed columns for consistency  

#### ✔ Email Normalization
- Converted to lowercase  
- Trimmed whitespace  

#### ✔ Timestamp Handling
- Parsed multiple formats using safe casting  

---

### 3️⃣ Data Merging

- Performed **FULL OUTER JOIN** on response datasets  
- Used **COALESCE** to handle missing values  
- Removed duplicate personal emails when GitHub ID matched  

---

### 4️⃣ Core Analysis

| Category        | Logic Used |
|----------------|----------|
| ✅ Valid        | INNER JOIN with student master |
| ❌ Invalid      | LEFT ANTI JOIN |
| 🚫 Not Submitted | Students not present in submissions |

---

### 5️⃣ Duplicate Detection

Used Window Function:

```sql
ROW_NUMBER() OVER (PARTITION BY student_id ORDER BY submission_time)

Day4/
│
├── raw_data/
│   ├── students.csv
│   ├── responses.csv
│   └── responses2.csv
│
├── notebooks/
│   └── solution.ipynb
│
├── src/
│   └── pipeline.py
│
└── README.md


📊 Output / Results
Cleaned and unified dataset
Valid vs Invalid submission identification
Duplicate submission detection
Final classified dataset for all students
