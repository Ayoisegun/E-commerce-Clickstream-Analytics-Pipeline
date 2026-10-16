from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull
from pyspark.sql import functions as F
import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, FloatType, NumericType
from functools import reduce

spark = SparkSession.builder \
    .appName("ecommerce_anaytics") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

schema = StructType(
    [
        StructField("event_id", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("brand", StringType(), True),
        StructField("category", StringType(), True),
        StructField("price", DoubleType(), True),
        StructField("timestamp", TimestampType(), True),
    ]
)

df = (
    spark.read.schema(schema)
    .option("corruptRecordColumn", "_corrupt_record")
    .json("clickstream_events.jsonl")
)
# df.show(truncate=False)
df = df.dropna(subset=["event_type"])
# analysis = df.filter(F.col("event_id").isNull()).show()
df = df.drop_duplicates()
# duplicate_rows = df.groupBy(df.columns).count().filter(col("count") > 1)
# duplicate_rows.show()
df.filter(
    F.col("category").isNull() & (F.col("event_type") != "page_view")
).count()

# df.filter(
#     F.col("price").isNotNull() & (F.col("event_type") == "unknown_click_action")
# ).show()



catalog_df = spark.read.option("header", "true").csv("product_catalog.csv")

catalog_lookup = catalog_df.select(
    F.col("id").alias("cat_product_id"),
    F.col("brand").alias("cat_brand"),
    F.col("category").alias("cat_category")
)

# 2. Perform a left join with the catalog
# (Using broadcast if your product catalog is a small-to-medium dimension table)
joined_df = df.join(
    F.broadcast(catalog_lookup),
    df["product_id"] == catalog_lookup["cat_product_id"],
    "left"
)

# 3. Define the condition: event_type is NOT null AND NOT 'page_view'
# (Also handling literal string 'NULL' if your data contains text instead of actual nulls)
valid_event_condition = (
    F.col("event_type").isNotNull() & 
    (F.col("event_type") != "NULL") & 
    (F.col("event_type") != "page_view")
)

# 4. Conditionally replace brand and category where they are null
updated_df = joined_df.withColumn(
    "brand",
    F.when(
        valid_event_condition & (F.col("brand").isNull() | (F.col("brand") == "NULL")),
        F.col("cat_brand")
    ).otherwise(F.col("brand"))
).withColumn(
    "category",
    F.when(
        valid_event_condition & (F.col("category").isNull() | (F.col("category") == "NULL")),
        F.col("cat_category")
    ).otherwise(F.col("category"))
).drop("cat_product_id", "cat_brand", "cat_category")

updated_df.filter(
    F.col("brand").isNull() & (F.col("event_type") == "purchase")
).show(truncate=False)

updated_df.filter(
    F.col("brand").isNull() & (F.col("event_type") == "product_view")
).show(truncate=False)

updated_df.filter(
    F.col("brand").isNull() & (F.col("event_type") == "add_to_cart")
).show(truncate=False)

updated_df = updated_df.filter(
    (F.col("price") >= 0) | (F.col("price") <= 1000)
)
updated_df = updated_df.dropDuplicates(["event_id"])

purchase_sessions = df.filter(F.col("event_type") == "purchase").select("session_id").distinct()
cart_sessions = df.filter(F.col("event_type") == "add_to_cart").select("session_id").distinct()

orphan_purchases = purchase_sessions.subtract(cart_sessions)
# orphan_purchases.show()


# orphan_records = updated_df.join(
#     orphan_purchases, 
#     on="session_id", 
#     how="left_semi"
# )
# orphan_records.show()

updated_df = updated_df.join(
    orphan_purchases, 
    on="session_id", 
    how="left_anti"
)

string_cols = ["event_type", "category", "brand"]

for c in string_cols:
    updated_df = updated_df.withColumn(c, F.trim(F.lower(F.col(c))))


price_check = (
    df.filter(F.col("event_type") == "purchase")
    .join(catalog_df, df.product_id == catalog_df.id, "inner")
    .select(
        df.event_id,  # <-- Added this!
        df.product_id,
        df.price.alias("event_price"),
        catalog_df.price.cast("double").alias("catalog_price"),
    )
    .withColumn("price_diff", F.abs(F.col("event_price") - F.col("catalog_price")))
)

# Filter for drifted prices and show them
drifted_prices = price_check.filter(F.col("price_diff") > 50)


#Clean updated_df by dropping those drifted purchase records using left_anti
updated_df = updated_df.join(
    drifted_prices,
    on="event_id",
    how="left_anti"
)