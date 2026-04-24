from pyspark import pipelines as dp
from pyspark.sql import functions as F

# ------------------------------------------------------------
# GOLD TABLE 1: Sales by Platform & Genre
# Business question: Which platform+genre combos dominate global sales?
# ------------------------------------------------------------
@dp.table
def gold_sales_by_platform_genre():
    s = dp.read("silver_vgsales_clean")

    return (
        s.groupBy("Platform", "Genre")
         .agg(
             F.count("*").alias("game_count"),
             F.round(F.sum("Global_Sales"), 2).alias("global_sales_total"),
             F.round(F.sum("NA_Sales"), 2).alias("na_sales_total"),
             F.round(F.sum("EU_Sales"), 2).alias("eu_sales_total"),
             F.round(F.sum("JP_Sales"), 2).alias("jp_sales_total"),
             F.round(F.sum("Other_Sales"), 2).alias("other_sales_total"),
         )
         .orderBy(F.desc("global_sales_total"))
    )


# ------------------------------------------------------------
# GOLD TABLE 2: Top Publishers by Global Sales
# Business question: Which publishers have the biggest footprint?
# ------------------------------------------------------------
@dp.table
def gold_top_publishers_by_global_sales():
    s = dp.read("silver_vgsales_clean")

    return (
        s.groupBy("Publisher")
         .agg(
             F.count("*").alias("total_titles"),
             F.round(F.sum("Global_Sales"), 2).alias("global_sales_total"),
             F.round(F.avg("Global_Sales"), 3).alias("avg_global_sales_per_title"),
         )
         .orderBy(F.desc("global_sales_total"))
    )


# ------------------------------------------------------------
# GOLD TABLE 3: Yearly Sales Trend (Global + titles)
# Business question: How did the market evolve over time?
# ------------------------------------------------------------
@dp.table
def gold_yearly_sales_trend():
    s = dp.read("silver_vgsales_clean")

    return (
        s.groupBy("Year")
         .agg(
             F.count("*").alias("titles_released"),
             F.round(F.sum("Global_Sales"), 2).alias("global_sales_total"),
             F.round(F.avg("Global_Sales"), 3).alias("avg_global_sales_per_title"),
         )
         .orderBy("Year")
    )
