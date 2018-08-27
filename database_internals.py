# information_schema.tables
conn = psycopg2.connect(dbname="dbname", user="user", password="eRqg123EEkl")
cur = conn.cursor()
cur.execute("SELECT table_name FROM information_schema.tables ORDER BY table_name")
table_names = cur.fetchall()
for name in table_names:
    print(name)

# find user created table
conn = psycopg2.connect(dbname="dbname", user="user", password="eRqg123EEkl")
cur = conn.cursor()
cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public' ORDER BY table_name")
for table_name in cur.fetchall():
    name = table_name[0]
    print(name)

# check table
conn = psycopg2.connect(dbname="dbname", user="user", password="eRqg123EEkl")
cur = conn.cursor()

cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
#for table in cur.fetchall():
    # Enter your code here...
from psycopg2.extensions import AsIs

cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
for table in cur.fetchall():
    table = table[0]
    cur.execute("SELECT * FROM %s LIMIT 0", [AsIs(table)])

# check datatype
conn = psycopg2.connect(dbname="dbname", user="user", password="eRqg123EEkl")
cur = conn.cursor()
cur.execute("SELECT oid, typname FROM pg_catalog.pg_type")
type_mappings = {
    int(oid): typname for oid, typname in cur.fetchall()
}

# count rows in each table
from psycopg2.extensions import AsIs
conn = psycopg2.connect(dbname="dbname", user="user", password="eRqg123EEkl")
cur = conn.cursor()
for table in readable_description.keys():
    cur.execute("SELECT COUNT(*) FROM %s", [AsIs(table)])
    readable_description[table]["total"] = cur.fetchone()

# limit rows
from psycopg2.extensions import AsIs
conn = psycopg2.connect(dbname="dbname", user="user", password="eRqg123EEkl")
cur = conn.cursor()
for table in readable_description.keys():
    cur.execute("SELECT * FROM %s LIMIT 100", [AsIs(table)])
    readable_description[table]["sample_rows"] = cur.fetchall()
