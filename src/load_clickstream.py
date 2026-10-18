from clean_clickstream import process_clickstream
from pyspark.sql import SparkSession
from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine
from pyspark.storagelevel import StorageLevel

os.environ["HADOOP_HOME"] = "C:\\hadoop"

load_dotenv()

PG_URL = f"jdbc:postgresql://{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
PG_PROPS = {
    "user": os.getenv("PG_USER"),
    "password": os.getenv("PG_PASSWORD"),
    "driver": "org.postgresql.Driver",
}
PG_TABLE = "cleaned_clickstream"

# Initialize Spark Session with PostgreSQL driver package
spark = SparkSession.builder \
    .appName("ecommerce_analytics") \
    .master("local[*]") \
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3") \
    .getOrCreate()

# 1. Clean data using your function
cleaned_df = process_clickstream(spark)
cleaned_df.show(20, truncate=False)

cleaned_df.cache()

# Trigger an action to materialize the cache (optional, but forces computation now)
cleaned_df.count()
cleaned_df.persist(StorageLevel.MEMORY_AND_DISK)

# 2. Write Spark DataFrame to PostgreSQL
cleaned_df.write \
    .option("truncate", True) \
    .mode("overwrite") \
    .jdbc(url=PG_URL, table=PG_TABLE, properties=PG_PROPS)

# 3. Load product catalog via Pandas / SQLAlchemy
engine = create_engine(
    f"postgresql+psycopg2://{os.getenv('PG_USER')}:{os.getenv('PG_PASSWORD')}"
    f"@{os.getenv('PG_HOST')}:{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
)

catalog_pd = pd.read_csv("product_catalog.csv")
catalog_pd = catalog_pd.rename(columns={
    "id": "product_id",
    "name": "product_name"
})
catalog_pd.to_sql("products", engine, if_exists="replace", index=False)

print("Data successfully loaded to PostgreSQL!")