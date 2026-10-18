from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, when, isnan, isnull
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"

def process_clickstream(spark: SparkSession):
    # 1. Define schema and read raw clickstream data
    schema = StructType([
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
    ])

    df = (
        spark.read.schema(schema)
        .option("corruptRecordColumn", "_corrupt_record")
        .json("clickstream_events.jsonl")
    )

    # 2. Basic drop nulls and exact duplicates
    df = df.dropna(subset=["event_type"])
    df = df.drop_duplicates()

    # 3. Read product catalog and create lookup
    catalog_df = spark.read.option("header", "true").csv("product_catalog.csv")
    catalog_lookup = catalog_df.select(
        F.col("id").alias("cat_product_id"),
        F.col("brand").alias("cat_brand"),
        F.col("category").alias("cat_category")
    )

    # 4. Join with catalog to impute missing brands/categories
    joined_df = df.join(
        F.broadcast(catalog_lookup),
        df["product_id"] == catalog_lookup["cat_product_id"],
        "left"
    )

    valid_event_condition = (
        F.col("event_type").isNotNull() & 
        (F.col("event_type") != "NULL") & 
        (F.col("event_type") != "page_view")
    )

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

    # 5. Filter valid prices (using '&' to keep range 0 to 1000)
    updated_df = updated_df.filter(
    ~(
        (F.col("event_type").isin("purchase", "add_to_cart")) &
        ((F.col("price") < 0) | (F.col("price") > 1000))
    )
)

    # 6. Drop duplicate event IDs
    updated_df = updated_df.dropDuplicates(["event_id"])

    # 7. Remove orphan purchases (purchases without prior add_to_cart in session)
    purchase_sessions = updated_df.filter(F.col("event_type") == "purchase").select("session_id").distinct()
    cart_sessions = updated_df.filter(F.col("event_type") == "add_to_cart").select("session_id").distinct()
    orphan_purchases = purchase_sessions.subtract(cart_sessions)

    orphan_purchase_ids = (
    df.filter(F.col("event_type") == "purchase")
      .join(orphan_purchases, on="session_id", how="inner")
      .select("event_id")
      .distinct()
)

    updated_df = updated_df.join(
    orphan_purchase_ids,
    on="event_id",
    how="left_anti"
)

    # 8. Clean and standardize strings (trim and lower)
    string_cols = ["event_type", "category", "brand"]
    for c in string_cols:
        updated_df = updated_df.withColumn(c, F.trim(F.lower(F.col(c))))

    # 9. Remove catalog price drifts (> 50 difference)
    # price_check = (
    #     updated_df.filter(F.col("event_type") == "purchase")
    #     .join(catalog_df, df.product_id == catalog_df.id, "inner")
    #     .select(
    #         updated_df.event_id,
    #         updated_df.product_id,
    #         df.price.alias("event_price"),
    #         catalog_df.price.cast("double").alias("catalog_price"),
    #     )
    #     .withColumn("price_diff", F.abs(F.col("event_price") - F.col("catalog_price")))
    # )

    updated_alias = updated_df.alias("u")
    catalog_alias = catalog_df.alias("c")

    price_check = (
        updated_alias
        .filter(F.col("u.event_type") == "purchase")
        .join(
            catalog_alias,
            F.col("u.product_id") == F.col("c.id"),
            "inner"
        )
        .select(
            F.col("u.event_id"),
            F.col("u.product_id"),
            F.col("u.price").alias("event_price"),
            F.col("c.price").cast("double").alias("catalog_price"),
        )
        .withColumn("price_diff", F.abs(F.col("event_price") - F.col("catalog_price")))
    )

    drifted_prices = price_check.filter(F.col("price_diff") > 50)

    updated_df = updated_df.join(
        drifted_prices,
        on="event_id",
        how="left_anti"
    )

    return updated_df

# Initialize Spark Session with PostgreSQL driver package
spark = SparkSession.builder \
    .appName("ecommerce_analytics") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()
cleaned_df = process_clickstream(spark)
print(cleaned_df.count())