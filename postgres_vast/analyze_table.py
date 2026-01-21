import psycopg2
import pandas as pd

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    dbname="gpu_marketplace",
    user="krish"
)

query = """SELECT * FROM public.gpu_offers_current;"""

df = pd.read_sql(query, conn)

pd.set_option("display.max_columns", None)
pd.set_option("display.width", 160)
pd.set_option("display.max_colwidth", None)

print(df)

conn.close()

