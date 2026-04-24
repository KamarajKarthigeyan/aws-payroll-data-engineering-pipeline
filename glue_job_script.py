import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_date, year, month, dayofmonth, round, sum, avg, max, min, coalesce, lit
from pyspark.sql import Row
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Initialize Spark and GlueContext
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

# Job Initialization
args = getResolvedOptions(sys.argv, ["JOB_NAME"])
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

print(f"Starting job: {args['JOB_NAME']}")

# Read from source
print("Reading source tables...")
sales_df = spark.read.table("sal_db.sal_sales")
region_df = spark.read.table("sal_db.reg_region")

print(f"Sales records: {sales_df.count()}")
print(f"Region records: {region_df.count()}")

# Clean column names
sales_df = sales_df.toDF(*[c.strip().replace(" ", "") for c in sales_df.columns])
region_df = region_df.toDF(*[c.strip().replace(" ", "") for c in region_df.columns])

# Alias tables for explicit column selection
sales_df = sales_df.alias("sales")
region_df = region_df.alias("region")

# Join with explicit column selection
print("Joining tables...")
df_joined = sales_df.join(
    region_df,
    on="region",
    how="left"
).select(
    col("sales.region"),
    col("sales.country"),  # Explicitly use country from sales table
    col("region.market"),
    col("sales.item_type"),
    col("sales.units_sold"),
    col("sales.unit_price"),
    col("sales.unit_cost")
)

print(f"Joined records: {df_joined.count()}")

# Add calculated columns with NULL handling
df_joined = df_joined.withColumn("revenue", 
                                 coalesce(col("units_sold"), lit(0)) * coalesce(col("unit_price"), lit(0.0)))\
                     .withColumn("profit", 
                                 col("revenue") - (coalesce(col("units_sold"), lit(0)) * coalesce(col("unit_cost"), lit(0.0))))

# Aggregations
print("Performing aggregations...")
df_agg = df_joined.groupBy("region", "country", "market", "item_type").agg(
    sum("revenue").alias("total_revenue"),
    sum("profit").alias("total_profit"),
    avg("unit_price").alias("avg_unit_price"),
    max("unit_price").alias("max_price"),
    min("unit_price").alias("min_price")
)

print(f"Aggregated records: {df_agg.count()}")

# Partition columns
df_agg = df_agg.withColumn("ingestion_date", current_date())\
               .withColumn("year", year(col("ingestion_date")))\
               .withColumn("month", month(col("ingestion_date")))\
               .withColumn("day", dayofmonth(col("ingestion_date")))

# Avoid small file issues
df_agg = df_agg.repartition(1)

# Writing to target table
print("Writing to target table...")
df_agg.write.mode("append")\
        .format("parquet")\
        .partitionBy("year", "month", "day")\
        .option("path", "s3://dz-demo-mt/processed/sales_joined/")\
        .saveAsTable("sal_db.sal_join_stg")

print("Data written successfully")

# Update partitions
spark.sql("MSCK REPAIR TABLE sal_db.sal_join_stg")
print("Partitions updated")

job.commit()
print("Job completed successfully")
