# login with password
import psycopg2
conn = psycopg2.connect(dbname="dbname", user="user", password="password")
print(conn)


# create user
conn = psycopg2.connect(dbname="dbname", user="user", password="password")
cur = conn.cursor()
cur.execute("CREATE USER username WITH PASSWORD 'somepassword' NOSUPERUSER")
conn.commit()

# revoke permission from user
conn = psycopg2.connect(dbname="dbname", user="user")
cur = conn.cursor()
cur.execute("REVOKE ALL ON tablename FROM username")
conn.commit()

# grant permission to user
conn = psycopg2.connect(dbname="dbname", user="user")
cur = conn.cursor()
cur.execute("GRANT SELECT ON tablename TO username")
conn.commit()

# create read-only user group
conn = psycopg2.connect(dbname="dbname", user="user")
cur = conn.cursor()
cur.execute("CREATE GROUP readonly NOLOGIN")
cur.execute("REVOKE ALL ON tablename FROM readonly")
cur.execute("GRANT SELECT ON tablename TO readonly")
cur.execute("GRANT readonly TO username")
conn.commit()

# create database
conn = psycopg2.connect(dbname="dbname", user="user")
# Connection must be set to autocommit.
conn.autocommit = True
cur = conn.cursor()
cur.execute("CREATE DATABASE databasename OWNER username")

# create database and table, then grant permissions
conn = psycopg2.connect(dbname="dbname", user="user")
conn.autocommit = True
cur = conn.cursor()
cur.execute("CREATE DATABASE top_secret OWNER user")
conn = psycopg2.connect(dbname="top_secret", user="user")
cur = conn.cursor()
cur.execute("""
CREATE TABLE documents(id INT, info TEXT);
CREATE GROUP spies NOLOGIN;
REVOKE ALL ON documents FROM spies;
GRANT SELECT, INSERT, UPDATE ON documents TO spies;
CREATE USER double_o_7 WITH CREATEDB PASSWORD 'shakennotstirred' IN GROUP spies;
""")
conn.commit()
conn_007 = psycopg2.connect(dbname='top_secret', user='double_o_7', password='shakennotstirred')
