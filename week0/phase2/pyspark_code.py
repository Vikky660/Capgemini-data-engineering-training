from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count

# initialize spark session
spark = SparkSession.builder.appName("phase2_analysis").getOrCreate()

# read datasets
cust_df = spark.read.option("header", "true").csv("/samples/customers.csv")
sales_df = spark.read.option("header", "true").csv("/samples/sales.csv")

# clean data (remove null customer_id)
cust_df = cust_df.dropna(subset=["customer_id"])
sales_df = sales_df.dropna(subset=["customer_id"])

# convert total_amount column to double
sales_df = sales_df.withColumn("total_amount", col("total_amount").cast("double"))

# total spending per customer
customer_spend = sales_df.groupBy("customer_id").agg(sum("total_amount").alias("total_spent"))
customer_spend.show()

# top 3 customers by spending
top_customers = customer_spend.orderBy(col("total_spent").desc()).limit(3)
top_customers.show()

# customers who have not placed any orders
customers_no_orders = cust_df.join(sales_df, on="customer_id", how="left_anti")
customers_no_orders.show()

# revenue generated per city
city_revenue = cust_df.join(sales_df, on="customer_id") \
                      .groupBy("city") \
                      .agg(sum("total_amount").alias("city_total"))
city_revenue.show()

# average order value per customer
avg_order_value = sales_df.groupBy("customer_id") \
                          .agg(avg("total_amount").alias("avg_order"))
avg_order_value.show()

# customers with multiple orders
repeat_customers = sales_df.groupBy("customer_id") \
                            .agg(count("*").alias("order_count")) \
                            .filter(col("order_count") > 1)
repeat_customers.show()

# customers sorted by total spending
sorted_spending = customer_spend.orderBy(col("total_spent").desc())
sorted_spending.show()
