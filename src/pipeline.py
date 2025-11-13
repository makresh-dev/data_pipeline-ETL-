import os
from pyspark.sql import SparkSession
from transformations.group_by import group_by_sales
from transformations.join_data import join_targets

# -----------------------
# FIXED PATH HANDLING
# -----------------------

ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT_DIR, "data")
OUTPUT_DIR = os.path.join(ROOT_DIR, "output")

sales_path = os.path.join(DATA_DIR, "sales.json")
targets_path = os.path.join(DATA_DIR, "region_targets.json")

# -----------------------
# UTILITY FUNCTIONS
# -----------------------

def print_json_output(spark, json_folder):
    """Reads and prints JSON output from the final_json folder."""
    print(f"\n🔍 Reading JSON from: {json_folder}")
    try:
        df = spark.read.json(json_folder)
        df.show(truncate=False)
    except Exception as e:
        print("❌ Error reading JSON:", e)


def print_parquet_output(spark, parquet_folder):
    """Reads and prints Parquet output from the final_parquet folder."""
    print(f"\n📦 Reading Parquet from: {parquet_folder}")
    try:
        df = spark.read.parquet(parquet_folder)
        df.show(truncate=False)
    except Exception as e:
        print("❌ Error reading Parquet:", e)

# -----------------------
# START SPARK
# -----------------------

spark = SparkSession.builder.appName("DataPipeline").getOrCreate()

# -----------------------
# READ INPUT FILES
# -----------------------

sales_df = spark.read.json(sales_path)
targets_df = spark.read.json(targets_path)

# -----------------------
# TRANSFORM
# -----------------------

grouped_df = group_by_sales(sales_df)
final_df = join_targets(grouped_df, targets_df)

# -----------------------
# WRITE OUTPUT
# -----------------------

json_path = os.path.join(OUTPUT_DIR, "final_json")
parquet_path = os.path.join(OUTPUT_DIR, "final_parquet")
gzip_path = os.path.join(OUTPUT_DIR, "final_gzip")

final_df.write.mode("overwrite").json(json_path)
final_df.write.mode("overwrite").parquet(parquet_path)
final_df.write.mode("overwrite").option("compression", "gzip").json(gzip_path)

print("\n🎉 Pipeline Finished Writing Outputs!")

# -----------------------
# PRINT OUTPUT DATA
# -----------------------

print_json_output(spark, json_path)
print_parquet_output(spark, parquet_path)

spark.stop()
