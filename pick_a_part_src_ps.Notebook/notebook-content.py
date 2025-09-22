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
# META     },
# META     "environment": {
# META       "environmentId": "e2a66915-fbf9-b5df-4d59-8200dd2a638a",
# META       "workspaceId": "00000000-0000-0000-0000-000000000000"
# META     }
# META   }
# META }

# CELL ********************

# %%
from notebookutils import mssparkutils
from pyspark.sql import functions as F

# -------- Config --------
SRC_PATH = "Files/pickapart/raw/vehicle/*/*.json"  # relative to attached Lakehouse
table_name = "vehicles_with_parts"
target_path = "abfss://0cd3d9a9-0414-44eb-91ad-5516f97df911@onelake.dfs.fabric.microsoft.com/a743e7da-a128-4060-a81f-4d6844a04abd/Tables/vehicles_with_parts"

# -------- Read + cache (multiline) --------
raw = (
    spark.read
         .option("multiLine", True)   # important: some files are pretty-printed
         .json(SRC_PATH)
         .withColumn("input_file", F.input_file_name())
         .cache()
)

# Touch the cache
_ = raw.count()
print("Raw columns:", raw.columns)

# -------- Inspect & drop corrupt rows safely --------
bad_count = 0
if "_corrupt_record" in raw.columns:
    bad = raw.filter(F.col("_corrupt_record").isNotNull()).select("input_file", "_corrupt_record")
    bad_count = bad.count()
    if bad_count > 0:
        print(f"⚠️ Found {bad_count} corrupt JSON file(s). Showing up to 50 paths:")
        for r in bad.limit(50).collect():
            print("-", r["input_file"])
    # keep only good rows
    raw = raw.filter(F.col("_corrupt_record").isNull()).drop("_corrupt_record")

good_count = raw.count()
assert good_count > 0, "All input rows are corrupt or empty. Fix/clean the source files and retry."
print(f"Proceeding with {good_count} valid rows.")

# -------- Build `flat` --------
flat = (
    raw
    # Parse scrape time and partition date
    .withColumn("scraped_at_ts", F.to_timestamp("scraped_at", "yyyyMMdd'T'HHmmss'Z'"))
    .withColumn("scraped_at_date", F.to_date("scraped_at_ts"))
    # Arrival date (e.g. '14 Feb 2025')
    .withColumn("arrival_date", F.to_date(F.col("details.date_in_stock"), "d MMM yyyy"))
    # Odometer as bigint (strip commas if present)
    .withColumn("odometer_clean", F.regexp_replace(F.col("details.odometer").cast("string"), ",", ""))
    .withColumn("odometer", F.col("odometer_clean").cast("bigint"))
    # Year as int
    .withColumn("year", F.col("details.year").cast("int"))
    # Explode parts (keep vehicles without parts)
    .withColumn("part_name", F.explode_outer("sold_parts"))
    .withColumn("part_name", F.when(F.col("part_name") == "Part:", F.lit(None)).otherwise(F.col("part_name")))
)

assert flat.limit(1).count() > 0, "No rows in `flat` after transforms."

# -------- Reset table storage --------
# 1) Drop table if exists
spark.sql(f"DROP TABLE IF EXISTS {table_name}")

# 2) Remove old path to avoid inheriting a broken _delta_log
try:
    mssparkutils.fs.rm(target_path, recurse=True)
    print("Removed existing folder:", target_path)
except Exception as e:
    print("Note: could not remove folder (may not exist):", e)

# -------- Write fresh Delta --------
(
    flat.write
        .format("delta")
        .mode("append")
        .option("overwriteSchema", "true")
        .partitionBy("scraped_at_date")
        .save(target_path)
)

# Register table on that location
spark.sql(f"CREATE TABLE {table_name} USING DELTA LOCATION '{target_path}'")

# -------- Retention (best-effort) --------
try:
    spark.sql(f"""
      ALTER TABLE {table_name}
      SET TBLPROPERTIES (
        'delta.logRetentionDuration' = 'interval 999999 days',
        'delta.deletedFileRetentionDuration' = 'interval 999999 days',
        'delta.enableExpiredLogCleanup' = 'false'
      )
    """)
except Exception as e:
    print("Note: retention properties not applied:", e)

# -------- Preview --------
display(spark.table(table_name).limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
