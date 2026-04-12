# 📊 Week 1 – Day 6: Car Sales Analytics Pipeline (Advanced PySpark + SQL)

---

## 🚀 Objective

Build a complete end-to-end data pipeline using **PySpark and SQL** by working with real-world datasets.  
The pipeline focuses on:

- Data cleaning and preprocessing  
- Handling invalid and inconsistent data  
- Performing multi-table joins  
- Generating business insights  
- Applying window functions  
- Designing a structured and scalable pipeline  

---

## 🧩 Problem Overview

This project simulates a real-world **data engineering use case** involving multiple datasets:

1. Customers Data  
2. Cars Data  
3. Sales Transactions  
4. Dealers Data  
5. Sales-Dealer Mapping  

---

## 🎯 Goals

- Clean and standardize raw datasets  
- Validate data integrity and relationships  
- Handle invalid and inconsistent records  
- Perform joins across multiple datasets  
- Generate business-level insights  
- Apply window functions for ranking and trends  
- Build a structured pipeline from raw data to analytics  

---

## 🛠️ Tech Stack

- PySpark  
- SQL (Spark SQL)  
- Databricks / Jupyter Notebook  

---

## 🔄 Data Pipeline Workflow

### 1️⃣ Data Ingestion

- Loaded CSV files into PySpark DataFrames  
- Created structured DataFrames for:
  - customers  
  - cars  
  - sales  
  - dealers  
  - sales_dealer  

---

### 2️⃣ Data Cleaning

Handled real-world data inconsistencies:

#### ✔ String Cleaning
- Trimmed extra spaces from text columns (name, city)

#### ✔ Data Type Conversion
- Converted `price` and `quantity` to numeric types  
- Parsed `sale_date` into proper date format  

#### ✔ Invalid Data Handling
- Converted negative car prices into absolute values  

#### ✔ Foreign Key Validation
- Removed records where:
  - `customer_id` not present in customers table  
  - `car_id` not present in cars table  

---

### 3️⃣ Data Validation

Performed validation checks to ensure data integrity:

#### 🔍 Orphan Detection (LEFT ANTI JOIN)
- Invalid customer references  
- Invalid car references  
- Invalid sales-dealer mappings  

#### 📊 Validation Reporting
- Counted invalid and inconsistent records  
- Generated summary reports for data quality  

---

### 4️⃣ Transformations

Built core analytical datasets:

#### 👤 Customer Revenue
- Total revenue per customer  
- Number of purchases  
- Total units purchased  

#### 🚗 Brand-wise Sales
- Total revenue per car brand  
- Number of sales transactions  
- Total units sold  
- Average price per brand  

#### 🌆 City-wise Revenue
- Aggregated revenue by customer city  

---

### 5️⃣ Dealer Analytics

Generated dealer-level insights:

#### 🏬 Dealer Revenue
- Total revenue per dealer  
- Number of transactions  
- Total units sold  

#### 🥇 Top Dealers
- Identified highest performing dealers  

#### 📍 Dealer Contribution by City
- Calculated percentage contribution of each dealer within their city  

---

### 6️⃣ SQL-Based Analysis

Performed advanced analysis using Spark SQL:

#### 🏆 Top 3 Customers per City
- Used `RANK()` window function  

#### 📅 Monthly Revenue Trends
- Aggregated revenue by year and month  

#### 🔁 Repeat Customers
- Identified customers with multiple purchases  

---

## 📁 Project Structure

``` id="d6structure001"
Day6/
│
├── raw_data/
│   ├── cars.csv
│   ├── customers.csv
│   ├── dealers.csv
│   ├── sales.csv
│   └── sales_dealer.csv
│
├── outputs/
│   ├── brand_sales/
│   ├── city_revenue/
│   ├── customer_revenue/
│   ├── dealer_revenue/
│   ├── monthly/
│   ├── repeat/
│   └── top3/
│
├── notebooks/
│   └── solution.ipynb
│
├── docs/
│   └── problem_statement.pdf
│
└── README.md
