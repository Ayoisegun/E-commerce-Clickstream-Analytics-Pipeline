import psycopg2
import os 
from dotenv import load_dotenv

load_dotenv()
print("Password found:", os.getenv("PG_PASSWORD") is not None)

# Connect to your Postgres database
conn = psycopg2.connect(
    dbname=os.getenv("PG_DB"),
    user=os.getenv("PG_USER"),
    password=os.getenv("PG_PASSWORD"),
    host="127.0.0.1",
    port=os.getenv("PG_PORT"),
)
cursor = conn.cursor()

# Read and execute the .sql file
with open("src/schema.sql", "r") as f:
    sql_script = f.read()
    cursor.execute(sql_script)

with open("src/product_schema.sql", "r") as f:
    sql_script = f.read()
    cursor.execute(sql_script)

# Commit changes and close connection
conn.commit()
cursor.close()
conn.close()

print("SQL script executed successfully!")