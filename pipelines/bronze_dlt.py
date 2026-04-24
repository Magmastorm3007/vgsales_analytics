from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table
def bronze_vgsales():
    # --- Ingesting raw files scraped from vgsales website ---
    catalog = spark.sql("SELECT current_catalog()").first()[0]
    path = f"/Volumes/{catalog}/default/raw_files/vgsales.csv"
    df=spark.read.format("csv")\
      .option("header", "true")\
      .option("inferSchema", "true")\
      .load(path)\
      .select(
              "*",
              F.current_timestamp().alias("_ingest_ts"),
              F.col("_metadata.file_path").alias("_source_file_path"),
              F.col("_metadata.file_name").alias("_source_file_name"),
              F.col("_metadata.file_modification_time").alias("_source_file_mod_time")
          )
    return df
