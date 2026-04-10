import psycopg2

conn = psycopg2.connect(
    dbname="postgres",
    user="postgres",
    password="postgres",
    host="localhost",
    port="5432"
)

conn.autocommit = True
cur = conn.cursor()

cur.execute("SELECT 1 FROM pg_database WHERE datname = 'jobs';")
exists = cur.fetchone()

if not exists:
    cur.execute("CREATE DATABASE jobs;")
    print("Database 'jobs' created.")
else:
    print("Database 'jobs' already exists.")

cur.close()
conn.close()