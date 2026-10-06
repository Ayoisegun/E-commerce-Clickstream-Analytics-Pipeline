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

# Read JSONL with explicit schema
df = (
    spark.read.schema(schema)
    .option("corruptRecordColumn", "_corrupt_record")
    .json("clickstream_events.jsonl")
)
df.show(truncate=False)

# Get column names
df.columns

# Get data types
df.dtypes

# Detailed schema information
df.schema

# Number of rows and columns
print(f"Rows: {df.count()}, Columns: {len(df.columns)}")
df.printSchema()

# Categorical column analysis
df.groupBy("event_type").count().orderBy("count", ascending=False).show()

# Distinct count
df.select("event_type").distinct().count()

from pyspark.sql import functions as F

# 1. Identify numeric columns that support NaN checks
numeric_cols = [
    field.name
    for field in df.schema.fields
    if isinstance(field.dataType, (FloatType, DoubleType, NumericType))
]

# 2. Build expressions dynamically based on column data type
expressions = []
for c in df.columns:
    if c in numeric_cols:
        # Check both NULL and NaN for numeric columns
        expr = (F.col(c).isNull() | F.isnan(F.col(c))).cast("int")
    else:
        # Check only NULL for non-numeric columns (strings, timestamps, etc.)
        expr = F.col(c).isNull().cast("int")

    expressions.append(F.sum(expr).alias(f"{c}_nulls"))

# 3. Compute counts in a single pass
df_null_counts = df.select(expressions)
df_null_counts.show(truncate=False)

# Build vertical summary table
null_counts_dict = df.select(
    [
        (
            F.sum((F.col(c).isNull() | F.isnan(F.col(c))).cast("int"))
            if c in numeric_cols
            else F.sum(F.col(c).isNull().cast("int"))
        ).alias(c)
        for c in df.columns
    ]
).collect()[0].asDict()

total_rows = df.count()

summary_df = spark.createDataFrame(
    [
        (col_name, count, round((count / total_rows) * 100, 2))
        for col_name, count in null_counts_dict.items()
    ],
    ["column_name", "null_or_nan_count", "missing_pct"],
)

summary_df.show(truncate=False)


def missingness_by_group(df, group_col, target_cols):
    """Show null rate per group — reveals structural/systematic nulls."""
    # 2. FIXED: Changed count(...) to sum(...)
    exprs = [
        count("*").alias("n"),
        *[
            F.sum(when(isnull(col(c)), 1).otherwise(0)).alias(f"{c}_nulls")
            for c in target_cols
        ],
    ]
    result = df.groupBy(group_col).agg(*exprs)

    # Convert to percentage
    for c in target_cols:
        result = result.withColumn(
            f"{c}_pct",
            when(col("n") > 0, (col(f"{c}_nulls") / col("n") * 100)).otherwise(0),
        )

    return result.orderBy(group_col)


# Run missingness evaluation
missingness_by_group(
    df, "event_type", ["product_id", "product_name", "brand", "price"]
).show(truncate=False)

# Filter out null categories/brands and collect unique brands per category
category_brands_df = (
    df.filter(F.col("category").isNotNull() & F.col("brand").isNotNull())
    .groupBy("category")
    .agg(
        F.collect_set("brand").alias("brands"),
        F.size(F.collect_set("brand")).alias("brand_count"),
    )
    .orderBy("category")
)

category_brands_df.show(truncate=False)

# Exact row duplicates
duplicate_count = df.count() - df.dropDuplicates().count()
print(f"Exact Duplicate Rows: {duplicate_count}")

# ID-level duplicates (same event_id logged multiple times)
event_id_dupes = (
    df.groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
)
print(f"Duplicate Event IDs: {event_id_dupes.count()}")

if "_corrupt_record" in df.columns:
    corrupt_df = df.filter(F.col("_corrupt_record").isNotNull())
    print(f"Corrupted JSON Rows: {corrupt_df.count()}")
    corrupt_df.select("_corrupt_record").show(10, truncate=False)

ranges_df = df.agg(
    F.min("timestamp").alias("earliest_timestamp"),
    F.max("timestamp").alias("latest_timestamp"),
    F.min("price").alias("min_price"),
    F.max("price").alias("max_price"),
    F.round(F.avg("price"), 2).alias("avg_price"),
)

print("--- Overall Dataset Ranges ---")
ranges_df.show(truncate=False)

# 2. Detailed Price Range Breakdown by Event Type
# (Excludes records where price is naturally null, like page_views)
price_by_event = (
    df.filter(F.col("price").isNotNull())
    .groupBy("event_type")
    .agg(
        F.count("price").alias("record_count"),
        F.min("price").alias("min_price"),
        F.max("price").alias("max_price"),
        F.round(F.avg("price"), 2).alias("avg_price"),
    )
    .orderBy("event_type")
)

print("--- Price Ranges by Event Type ---")
price_by_event.show(truncate=False)

# Check percentiles to spot sharp inflection points
percentiles = df.filter(F.col("price").isNotNull()).approxQuantile(
    "price", [0.50, 0.90, 0.95, 0.99, 0.999, 1.0], 0.001
)

percentile_names = ["p50 (Median)", "p90", "p95", "p99", "p99.9", "p100 (Max)"]
for name, val in zip(percentile_names, percentiles):
    print(f"{name:15s}: ${val:,.2f}")

# Analyze price ranges grouped by product category
category_stats = (
    df.filter(F.col("price").isNotNull())
    .groupBy("category")
    .agg(
        F.count("price").alias("count"),
        F.min("price").alias("min_price"),
        F.expr("percentile_approx(price, 0.50)").alias("median_price"),
        F.expr("percentile_approx(price, 0.99)").alias("p99_price"),
        F.max("price").alias("max_price"),
    )
    .orderBy("p99_price", ascending=False)
)

category_stats.show(truncate=False)

# Join events with standard catalog price to detect catalog drift/corruption
catalog_df = spark.read.option("header", "true").csv("product_catalog.csv")

price_check = (
    df.filter(F.col("event_type") == "purchase")
    .join(catalog_df, df.product_id == catalog_df.id, "inner")
    .select(
        df.product_id,
        df.price.alias("event_price"),
        catalog_df.price.cast("double").alias("catalog_price"),
    )
    .withColumn("price_diff", F.abs(F.col("event_price") - F.col("catalog_price")))
)

# Flag transactions where logged price varies wildly from catalog listing
price_check.filter(F.col("price_diff") > 50).show()

# Session length distribution (events per session)
session_length = (
    df.groupBy("session_id")
    .agg(
        F.count("*").alias("session_event_count"),
        F.countDistinct("event_type").alias("distinct_event_types")
    )
)

print("--- Session Length Statistics ---")
session_length.select(
    F.min("session_event_count"),
    F.expr("percentile_approx(session_event_count, 0.50)").alias("median_events"),
    F.max("session_event_count")
).show()

# Check for 'orphan' purchases (purchase with no add_to_cart in session)
purchase_sessions = df.filter(F.col("event_type") == "purchase").select("session_id").distinct()
cart_sessions = df.filter(F.col("event_type") == "add_to_cart").select("session_id").distinct()

orphan_purchases = purchase_sessions.subtract(cart_sessions)
print(f"Purchases without prior Add-to-Cart in same session: {orphan_purchases.count()}")

from pyspark.sql.window import Window

windowSpec = Window.partitionBy("session_id").orderBy("timestamp")

# Check time gap between consecutive events in a session
session_time_diff = df.withColumn(
    "prev_timestamp", F.lag("timestamp").over(windowSpec)
).withColumn(
    "time_gap_seconds",
    (F.col("timestamp").cast("double") - F.col("prev_timestamp").cast("double"))
)

# Identify negative time gaps (out-of-order late events)
out_of_order_events = session_time_diff.filter(F.col("time_gap_seconds") < 0)
print(f"Out-of-Order Events (Negative Time Gaps): {out_of_order_events.count()}")

string_cols = ["event_type", "category", "brand"]

for c in string_cols:
    dirty_strings = df.filter(
        (F.col(c) != F.trim(F.col(c))) | 
        (F.col(c) != F.lower(F.col(c)))
    )
    print(f"Untrimmed or Mixed-Case values in '{c}': {dirty_strings.count()}")