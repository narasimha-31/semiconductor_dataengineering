import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

conn = psycopg2.connect(
    host='localhost',
    port=int(os.getenv('POSTGRES_PORT')),
    dbname=os.getenv('POSTGRES_DB'),
    user=os.getenv('POSTGRES_USER'),
    password=os.getenv('POSTGRES_PASSWORD')
)

cursor = conn.cursor()
cursor.execute("SELECT version();")
result = cursor.fetchone()
print(f"Connected to: {result[0]}")

cursor.close()
conn.close()
