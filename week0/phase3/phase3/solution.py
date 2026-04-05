# imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, desc, rank
from pyspark.sql.window import Window

# initialize spark
spark = SparkSession.builder.appName("Sales_ETL_Workflow").getOrCreate()

# load datasets
cust_data = spark.read.csv("/samples/customers.csv", header=True, inferSchema=True)
orders_data = spark.read.csv("/samples/sales.csv", header=True, inferSchema=True)

# data cleaning
cust_data = cust_data.dropna()
orders_data = orders_data.dropna()

# remove invalid entries
orders_data = orders_data.filter(
    (col("quantity") > 0) & (col("total_amount") > 0)
)

# merge datasets
merged_df = orders_data.join(cust_data, on="customer_id")

# compute daily revenue
daily_revenue_df = orders_data.groupBy("sale_date") \
    .agg(sum("total_amount").alias("total_daily_sales"))

# compute revenue by city
revenue_by_city_df = merged_df.groupBy("city") \
    .agg(sum("total_amount").alias("total_city_revenue"))

# identify repeat buyers
frequent_buyers_df = orders_data.groupBy("customer_id") \
    .agg(count("*").alias("num_orders")) \
    .filter(col("num_orders") >= 2)

# calculate spending per customer within each city
customer_city_spend_df = merged_df.groupBy("customer_id", "city") \
    .agg(sum("total_amount").alias("customer_spending"))

# window function for ranking
city_window = Window.partitionBy("city").orderBy(desc("customer_spending"))

# top customer in each city
top_city_customers_df = customer_city_spend_df \
    .withColumn("rank_position", rank().over(city_window)) \
    .filter(col("rank_position") == 1)

# final aggregated report
summary_report_df = merged_df.groupBy("customer_id", "city") \
    .agg(
        sum("total_amount").alias("total_spending"),
        count("*").alias("order_frequency")
    )

# display outputs
daily_revenue_df.show()
revenue_by_city_df.show()
frequent_buyers_df.show()
top_city_customers_df.show()
summary_report_df.show()
