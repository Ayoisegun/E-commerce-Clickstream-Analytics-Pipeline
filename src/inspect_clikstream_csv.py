from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull
from pyspark.sql import functions as F
import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType, TimestampType, FloatType, NumericType
from functools import reduce
from pyspark.sql.window import Window
import csv
from datetime import datetime

# ---------------------------------------------------------------------------
# Quality report collector
# ---------------------------------------------------------------------------
quality_report = []


def add_metric(check_name, category, value, details="", status=""):
    """Append a metric row to the in-memory quality report."""
    quality_report.append({
        "check_name": check_name,
        "category": category,
        "value": value,
        "details": details,
        "status": status,
        "generated_at": datetime.now().isoformat(),
    })


def write_quality_report(report_rows, output_path="quality_report.csv"):
    """Write the collected quality metrics to a CSV file."""
    if not report_rows:
        print("No metrics collected.")
        return
    fieldnames = list(report_rows[0].keys())
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(report_rows)
    print(f"Quality report written to {output_path} ({len(report_rows)} rows)")


# ---------------------------------------------------------------------------
# Spark session
# ---------------------------------------------------------------------------
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
    .option("mode", "PERMISSIVE")
    .json("clickstream_events.jsonl")
)
df.show(truncate=False)

# Cache because we'll run many aggregations over the same frame
df.cache()

# Column names / data types / schema
df.columns
df.dtypes
df.schema

# ---------------------------------------------------------------------------
# 1. Basic shape
# ---------------------------------------------------------------------------
row_count = df.count()
col_count = len(df.columns)
print(f"Rows: {row_count}, Columns: {col_count}")
df.printSchema()

add_metric("row_count", "shape", row_count)
add_metric("column_count", "shape", col_count)

# ---------------------------------------------------------------------------
# 2. Categorical column analysis
# ---------------------------------------------------------------------------
event_type_counts = df.groupBy("event_type").count().orderBy("count", ascending=False)
event_type_counts.show()

for row in event_type_counts.collect():
    add_metric(
        check_name=f"event_type_count__{row['event_type']}",
        category="categorical",
        value=row["count"],
    )

distinct_event_types = df.select("event_type").distinct().count()
print(f"Distinct event_type values: {distinct_event_types}")
add_metric("distinct_event_types", "categorical", distinct_event_types)

# ---------------------------------------------------------------------------
# 3. Null / NaN summary
# ---------------------------------------------------------------------------
numeric_cols = [
    field.name
    for field in df.schema.fields
    if isinstance(field.dataType, (FloatType, DoubleType, NumericType))
]

expressions = []
for c in df.columns:
    if c in numeric_cols:
        expr = (F.col(c).isNull() | F.isnan(F.col(c))).cast("int")
    else:
        expr = F.col(c).isNull().cast("int")
    expressions.append(F.sum(expr).alias(f"{c}_nulls"))

df_null_counts = df.select(expressions)
df_null_counts.show(truncate=False)

# Vertical summary table
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

summary_df = spark.createDataFrame(
    [
        (col_name, cnt, round((cnt / row_count) * 100, 2))
        for col_name, cnt in null_counts_dict.items()
    ],
    ["column_name", "null_or_nan_count", "missing_pct"],
)
summary_df.show(truncate=False)

for row in summary_df.collect():
    add_metric(
        check_name=f"null_pct__{row['column_name']}",
        category="missingness",
        value=row["missing_pct"],
        details=f"null_or_nan_count={row['null_or_nan_count']}",
        status="WARN" if row["missing_pct"] > 5 else "OK",
    )


# ---------------------------------------------------------------------------
# 4. Missingness by group
# ---------------------------------------------------------------------------
def missingness_by_group(df, group_col, target_cols):
    """Show null rate per group — reveals structural/systematic nulls."""
    exprs = [
        count("*").alias("n"),
        *[
            F.sum(when(isnull(col(c)), 1).otherwise(0)).alias(f"{c}_nulls")
            for c in target_cols
        ],
    ]
    result = df.groupBy(group_col).agg(*exprs)

    for c in target_cols:
        result = result.withColumn(
            f"{c}_pct",
            when(col("n") > 0, (col(f"{c}_nulls") / col("n") * 100)).otherwise(0),
        )

    return result.orderBy(group_col)


missingness_result = missingness_by_group(
    df, "event_type", ["product_id", "product_name", "brand", "price"]
)
missingness_result.show(truncate=False)

for row in missingness_result.collect():
    for c in ["product_id", "product_name", "brand", "price"]:
        add_metric(
            check_name=f"null_pct_by_event_type__{c}__{row['event_type']}",
            category="missingness_by_group",
            value=round(row[f"{c}_pct"], 2),
            details=f"group_size={row['n']}",
            status="WARN" if row[f"{c}_pct"] > 5 else "OK",
        )

# ---------------------------------------------------------------------------
# 5. Category / brand cardinality
# ---------------------------------------------------------------------------
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

for row in category_brands_df.collect():
    add_metric(
        check_name=f"brand_count__{row['category']}",
        category="cardinality",
        value=row["brand_count"],
        details=",".join(sorted(row["brands"])[:5]),
    )

# ---------------------------------------------------------------------------
# 6. Duplicates
# ---------------------------------------------------------------------------
duplicate_count = row_count - df.dropDuplicates().count()
print(f"Exact Duplicate Rows: {duplicate_count}")
add_metric(
    "exact_duplicate_rows", "duplicates", duplicate_count,
    status="WARN" if duplicate_count > 0 else "OK",
)

event_id_dupes = (
    df.groupBy("event_id")
    .count()
    .filter(F.col("count") > 1)
)
event_id_dupes_count = event_id_dupes.count()
print(f"Duplicate Event IDs: {event_id_dupes_count}")
add_metric(
    "duplicate_event_ids", "duplicates", event_id_dupes_count,
    status="WARN" if event_id_dupes_count > 0 else "OK",
)

# ---------------------------------------------------------------------------
# 7. Corrupt JSON records
# ---------------------------------------------------------------------------
if "_corrupt_record" in df.columns:
    corrupt_df = df.filter(F.col("_corrupt_record").isNotNull())
    corrupt_count = corrupt_df.count()
    print(f"Corrupted JSON Rows: {corrupt_count}")
    corrupt_df.select("_corrupt_record").show(10, truncate=False)
    add_metric(
        "corrupt_json_rows", "integrity", corrupt_count,
        status="FAIL" if corrupt_count > 0 else "OK",
    )

# ---------------------------------------------------------------------------
# 8. Range statistics
# ---------------------------------------------------------------------------
ranges_df = df.agg(
    F.min("timestamp").alias("earliest_timestamp"),
    F.max("timestamp").alias("latest_timestamp"),
    F.min("price").alias("min_price"),
    F.max("price").alias("max_price"),
    F.round(F.avg("price"), 2).alias("avg_price"),
)
print("--- Overall Dataset Ranges ---")
ranges_df.show(truncate=False)

ranges_row = ranges_df.collect()[0]
for k, v in ranges_row.asDict().items():
    add_metric(f"range__{k}", "ranges", v)

# ---------------------------------------------------------------------------
# 9. Price ranges by event type
# ---------------------------------------------------------------------------
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

for row in price_by_event.collect():
    add_metric(
        f"price_range__{row['event_type']}", "price_by_event", row["avg_price"],
        details=f"min={row['min_price']}, max={row['max_price']}, n={row['record_count']}",
    )

# ---------------------------------------------------------------------------
# 10. Price percentiles
# ---------------------------------------------------------------------------
percentiles = df.filter(F.col("price").isNotNull()).approxQuantile(
    "price", [0.50, 0.90, 0.95, 0.99, 0.999, 1.0], 0.001
)
percentile_names = ["p50 (Median)", "p90", "p95", "p99", "p99.9", "p100 (Max)"]
for name, val in zip(percentile_names, percentiles):
    print(f"{name:15s}: ${val:,.2f}")
    add_metric(f"price_percentile__{name}", "distribution", round(val, 2))

# ---------------------------------------------------------------------------
# 11. Category price stats
# ---------------------------------------------------------------------------
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

for row in category_stats.collect():
    add_metric(
        f"category_price__{row['category']}", "category_prices", row["median_price"],
        details=f"min={row['min_price']}, p99={row['p99_price']}, max={row['max_price']}, n={row['count']}",
    )

# ---------------------------------------------------------------------------
# 12. Catalog drift
# ---------------------------------------------------------------------------
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

drift_df = price_check.filter(F.col("price_diff") > 50)
drift_count = drift_df.count()
drift_df.show()
add_metric(
    "catalog_price_drift_count", "integrity", drift_count,
    details="purchases where |event_price - catalog_price| > 50",
    status="WARN" if drift_count > 0 else "OK",
)

# ---------------------------------------------------------------------------
# 13. Session length distribution
# ---------------------------------------------------------------------------
session_length = (
    df.groupBy("session_id")
    .agg(
        F.count("*").alias("session_event_count"),
        F.countDistinct("event_type").alias("distinct_event_types"),
    )
)

print("--- Session Length Statistics ---")
session_stats = session_length.select(
    F.min("session_event_count").alias("min_events"),
    F.expr("percentile_approx(session_event_count, 0.50)").alias("median_events"),
    F.max("session_event_count").alias("max_events"),
).collect()[0]
session_stats_df = spark.createDataFrame([session_stats], session_stats.__fields__)
session_stats_df.show()

for k, v in session_stats.asDict().items():
    add_metric(f"session_length__{k}", "session", v)

# ---------------------------------------------------------------------------
# 14. Orphan purchases
# ---------------------------------------------------------------------------
purchase_sessions = df.filter(F.col("event_type") == "purchase").select("session_id").distinct()
cart_sessions = df.filter(F.col("event_type") == "add_to_cart").select("session_id").distinct()

orphan_purchases = purchase_sessions.subtract(cart_sessions)
orphan_count = orphan_purchases.count()
print(f"Purchases without prior Add-to-Cart in same session: {orphan_count}")
add_metric(
    "orphan_purchases", "integrity", orphan_count,
    details="purchases with no add_to_cart in same session",
    status="WARN" if orphan_count > 0 else "OK",
)

# ---------------------------------------------------------------------------
# 15. Out-of-order events
# ---------------------------------------------------------------------------
windowSpec = Window.partitionBy("session_id").orderBy("timestamp", "event_id")

session_time_diff = df.withColumn(
    "prev_timestamp", F.lag("timestamp").over(windowSpec)
).withColumn(
    "time_gap_seconds",
    (F.col("timestamp").cast("double") - F.col("prev_timestamp").cast("double")),
)

out_of_order_events = session_time_diff.filter(F.col("time_gap_seconds") < 0)
ooo_count = out_of_order_events.count()
print(f"Out-of-Order Events (Negative Time Gaps): {ooo_count}")
add_metric(
    "out_of_order_events", "integrity", ooo_count,
    status="WARN" if ooo_count > 0 else "OK",
)

# ---------------------------------------------------------------------------
# 16. Dirty strings
# ---------------------------------------------------------------------------
string_cols = ["event_type", "category", "brand"]

for c in string_cols:
    dirty_count = df.filter(
        (F.col(c) != F.trim(F.col(c))) |
        (F.col(c) != F.lower(F.col(c)))
    ).count()
    print(f"Untrimmed or Mixed-Case values in '{c}': {dirty_count}")
    add_metric(
        f"dirty_strings__{c}", "string_quality", dirty_count,
        status="WARN" if dirty_count > 0 else "OK",
    )

# ---------------------------------------------------------------------------
# Write the report
# ---------------------------------------------------------------------------
write_quality_report(quality_report, "quality_report.csv")

spark.stop()