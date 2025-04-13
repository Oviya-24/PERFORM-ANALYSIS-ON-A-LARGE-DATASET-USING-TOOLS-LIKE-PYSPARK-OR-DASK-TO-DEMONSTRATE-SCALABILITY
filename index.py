from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as _sum, desc

# 1. Initialize Spark Session
spark = SparkSession.builder \
    .appName("RetailDataAnalysis") \
    .config("spark.executor.memory", "2g") \
    .config("spark.driver.memory", "2g") \
    .getOrCreate()

# 2. Load dataset (assuming CSV for this example)
# Replace 'large_retail_dataset.csv' with your actual dataset path
df = spark.read.csv("large_retail_dataset.csv", header=True, inferSchema=True)

# Show schema to verify loading
df.printSchema()

# 3. Basic Cleaning: Drop nulls in important columns
df_clean = df.dropna(subset=["InvoiceNo", "StockCode", "Description", "Quantity", "UnitPrice", "Country"])

# 4. Create new column: TotalSales = Quantity * UnitPrice
df_sales = df_clean.withColumn("TotalSales", col("Quantity") * col("UnitPrice"))

# 5. Analysis 1: Total transactions per country
transactions_by_country = df_sales.groupBy("Country") \
    .count() \
    .orderBy(desc("count"))

# 6. Analysis 2: Top 5 products by total sales
top_products = df_sales.groupBy("Description") \
    .agg(_sum("TotalSales").alias("TotalRevenue")) \
    .orderBy(desc("TotalRevenue")) \
    .limit(5)

# 7. Show Results
print("=== Transactions per Country ===")
transactions_by_country.show()

print("=== Top 5 Products by Revenue ===")
top_products.show()

# Stop the Spark session
spark.stop()
