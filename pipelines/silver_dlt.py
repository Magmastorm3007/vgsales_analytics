from pyspark import pipelines as dp
from pyspark.sql import functions as F

@dp.table
@dp.expect("name_not_null", "Name IS NOT NULL AND Name <> ''")
@dp.expect("platform_not_null", "Platform IS NOT NULL AND Platform <> ''")
@dp.expect("year_reasonable", "Year BETWEEN 1980 AND 2030")
@dp.expect("global_sales_non_negative", "Global_Sales >= 0")
def silver_vgsales_clean():
    df = dp.read("bronze_vgsales")

    # 1) Standardize column names safely 
    df = df.select(*[F.col(c).alias(c.strip().replace(" ", "_")) for c in df.columns])

    # 2) Type casting + trimming
    df = (
        df
        .withColumn("Rank", F.col("Rank").cast("int"))
        .withColumn("Year", F.col("Year").cast("int"))
        .withColumn("NA_Sales", F.col("NA_Sales").cast("double"))
        .withColumn("EU_Sales", F.col("EU_Sales").cast("double"))
        .withColumn("JP_Sales", F.col("JP_Sales").cast("double"))
        .withColumn("Other_Sales", F.col("Other_Sales").cast("double"))
        .withColumn("Global_Sales", F.col("Global_Sales").cast("double"))
        .withColumn("Name", F.trim(F.col("Name")))
        .withColumn("Platform", F.trim(F.col("Platform")))
        .withColumn("Genre", F.trim(F.col("Genre")))
        .withColumn("Publisher", F.trim(F.col("Publisher")))
    )

    # 3) Validation filters 
    df = (
        df
        .filter(F.col("Name").isNotNull() & (F.col("Name") != ""))
        .filter(F.col("Platform").isNotNull() & (F.col("Platform") != ""))
        .filter(F.col("Year").isNotNull())
    )

    # 4) Deduplicate
    df = df.dropDuplicates(["Rank"])
    return df
    