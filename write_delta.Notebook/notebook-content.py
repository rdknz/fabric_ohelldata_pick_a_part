# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a743e7da-a128-4060-a81f-4d6844a04abd",
# META       "default_lakehouse_name": "pick_a_part",
# META       "default_lakehouse_workspace_id": "0cd3d9a9-0414-44eb-91ad-5516f97df911",
# META       "known_lakehouses": [
# META         {
# META           "id": "a743e7da-a128-4060-a81f-4d6844a04abd"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# %%
# Create a managed Delta table partitioned by scraped_at_date
# Schema evolves automatically from JSON input
# Lakehouse must be attached

from pyspark.sql import functions as F

# 1) Source path: all per-vehicle JSONs from crawler
src_path = "Files/pickapart/raw/vehicle/*/*.json"

# 2) Read JSON (schema inferred automatically)
raw = spark.read.option("multiLine", False).json(src_path)

# 3) Derive useful columns
flat = (
    raw
    # Parse scraped_at into timestamp + date partition
    .withColumn("scraped_at_ts", F.to_timestamp("scraped_at", "yyyyMMdd'T'HHmmss'Z'"))
    .withColumn("scraped_at_date", F.to_date("scraped_at_ts"))
    # Arrival date from nested details
    .withColumn("arrival_date", F.to_date(F.col("details.date_in_stock"), "d MMM yyyy"))
    # Odometer as bigint (strip commas if present)
    .withColumn("odometer_clean", F.regexp_replace(F.col("details.odometer"), ",", ""))
    .withColumn("odometer", F.col("odometer_clean").cast("bigint"))
    # Year as int
    .withColumn("year", F.col("details.year").cast("int"))
    # Explode sold_parts array into individual rows (keep vehicles without parts)
    .withColumn("part_name", F.explode_outer("sold_parts"))
    # Clean placeholder "Part:" rows
    .withColumn("part_name", F.when(F.col("part_name") == "Part:", F.lit(None)).otherwise(F.col("part_name")))
)

# 4) Create schema if not exists, then write as Delta
spark.sql("CREATE SCHEMA IF NOT EXISTS raw")

table_name = "raw.vehicles_with_parts"

(
    flat.write
        .format("delta")
        .mode("overwrite")            # use "append" for incremental loads
        .partitionBy("scraped_at_date")
        .saveAsTable(table_name)
)

# 5) Long retention settings
spark.sql(f"""
  ALTER TABLE {table_name}
  SET TBLPROPERTIES (
    'delta.logRetentionDuration' = 'interval 999999 days',
    'delta.deletedFileRetentionDuration' = 'interval 999999 days',
    'delta.enableExpiredLogCleanup' = 'false'
  )
""")

# Optional: safety if runtime enforces retention checks
spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")

# 6) Quick preview
display(spark.table(table_name).limit(5))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
