from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_timestamp 
from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine
os.environ["HADOOP_HOME"] = "C:\\hadoop"

load_dotenv()
PG_URL = f"jdbc:postgresql://{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
PG_PROPS = {
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "driver": "org.postgresql.Driver",
}
PG_TABLE = "raw_clickstream"

spark = SparkSession.builder \
    .appName("ecommerce_anaytics") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

commerce_df = spark.read.json("clickstream_events1.jsonl")
commerce_df.printSchema()
commerce_df = commerce_df.withColumn("timestamp", to_timestamp(col("timestamp")))
commerce_df = commerce_df.dropna(subset=["event_id", "user_id", "session_id", "event_type", "timestamp"])
commerce_df = commerce_df = commerce_df.withColumn("price", col("price").cast("decimal(10,2)"))

commerce_df.printSchema()

commerce_df.write \
    .option("truncate", "true")\
    .mode("overwrite") \
    .jdbc(url=PG_URL, table=PG_TABLE, properties=PG_PROPS)


engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}"
    f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
)
df = pd.read_csv("product_catalog.csv")
df = df.rename(columns={
    "id": "product_id",
    "name": "product_name"
})
df.to_sql("products", engine, if_exists="append", index=False)