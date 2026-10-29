import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.storagelevel import StorageLevel

from clean_clickstream import process_clickstream

os.environ["HADOOP_HOME"] = "C:\\hadoop"


def load_clickstream():
    load_dotenv()

    pg_url = (
        f"jdbc:postgresql://{os.getenv('PG_HOST')}:"
        f"{os.getenv('PG_PORT')}/{os.getenv('PG_DB')}"
    )
    pg_props = {
        "user": os.getenv("PG_USER"),
        "password": os.getenv("PG_PASSWORD"),
        "driver": "org.postgresql.Driver",
    }
    pg_table = "cleaned_clickstream"

    # Initialize Spark Session with PostgreSQL driver package
    spark = (
        SparkSession.builder
        .appName("ecommerce_analytics")
        .master("local[*]")
        .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
        .config("spark.driver.memoryOverhead", "256m")
        .config("spark.executor.memoryOverhead", "256m")
        .getOrCreate()
    )

    try:
        # 1. Clean data using the process_clickstream function
        cleaned_df = process_clickstream(spark)
        cleaned_df.show(20, truncate=False)

        # 2. Cache / persist the DataFrame
        cleaned_df.persist(StorageLevel.MEMORY_AND_DISK)
        cleaned_df.count()  # materialise the cache

        # 3. Write Spark DataFrame to PostgreSQL
        (
            cleaned_df.write
            .option("truncate", True)
            .mode("overwrite")
            .jdbc(url=pg_url, table=pg_table, properties=pg_props)
        )

        print("Data successfully loaded to PostgreSQL!")

        cleaned_df.unpersist()
    finally:
        spark.stop()


if __name__ == "__main__":
    load_clickstream()