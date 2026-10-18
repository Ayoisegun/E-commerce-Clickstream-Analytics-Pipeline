import os

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import URL


# 1. Load environment variables
load_dotenv()

required_vars = [
    "PG_HOST",
    "PG_PORT",
    "PG_DB",
    "PG_USER",
    "PG_PASSWORD",
]

missing_vars = [var for var in required_vars if not os.getenv(var)]

if missing_vars:
    raise ValueError(f"Missing environment variables: {missing_vars}")


# 2. Connect to PostgreSQL
db_url = URL.create(
    drivername="postgresql+psycopg2",
    username=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD"),
    host=os.getenv("PG_HOST"),
    port=int(os.getenv("PG_PORT")),
    database=os.getenv("PG_DB"),
)

engine = create_engine(db_url, pool_pre_ping=True)


try:
    # 3. Read product catalog
    products_df = pd.read_csv("product_catalog.csv")

    if products_df.empty:
        raise ValueError("Product catalog is empty.")

    # Standardise column names
    products_df.columns = products_df.columns.str.strip().str.lower()

    products_df = products_df.rename(columns={
        "id": "product_id",
        "name": "product_name",
    })

    # Validate required columns
    required_columns = {"product_id", "product_name"}
    missing_columns = required_columns - set(products_df.columns)

    if missing_columns:
        raise ValueError(f"Missing columns: {missing_columns}")

    # Validate product IDs
    if products_df["product_id"].isna().any():
        raise ValueError("Missing product IDs detected.")

    if products_df["product_id"].duplicated().any():
        raise ValueError("Duplicate product IDs detected.")

    print(f"Products in CSV: {len(products_df)}")
    print("\nCSV preview:")
    print(products_df.head().to_string(index=False))


    # 4. Check database connection
    with engine.connect() as conn:
        db_info = conn.execute(text("""
            SELECT current_database(), current_schema(), current_user
        """)).fetchone()

        print(f"\nDatabase: {db_info[0]}")
        print(f"Schema: {db_info[1]}")
        print(f"User: {db_info[2]}")


    # 5. Replace product records without dropping the table
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE TABLE public.products"))

        products_df.to_sql(
            name="products",
            con=conn,
            schema="public",
            if_exists="append",
            index=False,
            chunksize=5000,
            method="multi",
        )


    # 6. Verify the load
    with engine.connect() as conn:
        count = conn.execute(
            text("SELECT COUNT(*) FROM public.products")
        ).scalar_one()

        print(f"\nProducts loaded into PostgreSQL: {count}")

        if count != len(products_df):
            raise RuntimeError(
                f"Expected {len(products_df)} products, found {count}."
            )

        print("\nDatabase preview:")
        result = conn.execute(text("""
            SELECT *
            FROM public.products
            LIMIT 5
        """))

        for row in result.fetchall():
            print(row)

    print("\nProduct catalog loaded successfully.")


finally:
    engine.dispose()
