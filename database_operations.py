# connect to postgres SQL database
import psycopg2
conn = psycopg2.connect("dbname=dbname user=user")
print(conn)
conn.close()

# run a select query
conn = psycopg2.connect("dbname=dbname user=user")
cur = conn.cursor()
cur.execute('SELECT * FROM notes')
notes = cur.fetchall()
conn.close()

# create table
conn = psycopg2.connect("dbname=dbname user=user")
cur = conn.cursor()
cur.execute("CREATE TABLE users(id integer PRIMARY KEY, email text, name text, address text)")

# commit your operation
conn = psycopg2.connect("dbname=dbname user=user")
cur = conn.cursor()
cur.execute("CREATE TABLE users(id integer PRIMARY KEY, email text, name text, address text)")
conn.commit()
conn.close()

# insert into databse from a csv file
with open('user_accounts.csv') as f:
    reader = csv.reader(f)
    next(reader)
    rows = [row for row in reader]
conn = psycopg2.connect('dbname=dq user=dq')
cur = conn.cursor()
for row in rows:
    cur.execute('insert into users values (%s, %s, %s, %s)', row)
conn.commit()

cur.execute('select * from users')
users = cur.fetchall()
conn.close()

# copy from csv file
conn = psycopg2.connect("dbname=dq user=dq")
cur = conn.cursor()
cur.execute('drop table if exists users')
cur.execute("CREATE TABLE users(id integer PRIMARY KEY, email text, name text, address text)")
with open('user_accounts.csv') as f:
    next(f)  # Skip header.
    cur.copy_from(f, 'users', sep=",")
conn.commit()

cur.execute('SELECT * FROM users')
users = cur.fetchall()
conn.close()
print(users)

# user description
conn = psycopg2.connect("dbname=dbname user=user")
cur = conn.cursor()
cur.execute('select * from tablename limit 0')
print(cur.description)

# create table
conn = psycopg2.connect("dbname=dbname user=user")
cur = conn.cursor()
cur.execute( '''
create table ign_reviews (
id bigint primary key)
''')
conn.commit()

# create table
conn = psycopg2.connect("dbname=dbname user=user")
cur = conn.cursor()
import csv
from datetime import date
cur.execute('''
    create table ign_reviews (
        id bigint primary key,
        score_phrase varchar(11),
        title text,
        url text,
        platform varchar(20),
        score decimal(3,1),
        genre text,
        editors_choice boolean,
        release_date date
    )
''')
with open ('ign.csv') as f:
    next(f)
    reader = csv.reader(f)
    for row in reader:
        updated_row = row[:8]
        updated_row.append(date(int(row[8]), int(row[9]), int(row[10])))
        cur.execute('insert into ign_reviews values (%s, %s, %s, %s, %s, %s, %s, %s, %s)', updated_row)
conn.commit()

# alter table rename table name
conn = psycopg2.connect("dbname=dbname user=user")
cur = conn.cursor()
cur.execute('alter table old_ign_reviews rename to ign_reviews')
conn.commit()
cur.execute('select * from ign_reviews limit 0')
print(cur.description)

# alter table drop column
conn = psycopg2.connect("dbname=dbname user=user")
cur = conn.cursor()
cur.execute('alter table ign_reviews drop column full_url')
conn.commit()

# alter table change data type
conn = psycopg2.connect("dbname=dbname user=user")
cur = conn.cursor()
cur.execute('alter table ign_reviews alter column id type bigint')
conn.commit()

# alter table rename column name
conn = psycopg2.connect("dbname=dbname user=user")
cur = conn.cursor()
cur.execute('ALTER TABLE ign_reviews RENAME COLUMN title_of_game_review TO title')
conn.commit()

# alter table add column
conn = psycopg2.connect("dbname=dbname user=user")
cur = conn.cursor()
cur.execute('alter table ign_reviews add column release_date date')
conn.commit()

# update table set column
conn = psycopg2.connect("dbname=dbname user=user")
cur = conn.cursor()
cur.execute("UPDATE ign_reviews SET release_date = to_date(release_day || '-' || release_month || '-' || release_year, 'DD-MM-YYYY')")
conn.commit()

# read data from csv file
conn = psycopg2.connect("dbname=dbname user=user")
cur = conn.cursor()
with open('ign.csv', 'r') as f:
    next(f)
    reader = csv.reader(f)
    for row in reader:
        cur.execute(
            "insert into ign_reviews values (%s, %s, %s, %s, %s, %s ,%s, %s, %s)", row
        )
conn.commit()

# use mogrify
conn = psycopg2.connect("dbname=dbname user=user")
cur = conn.cursor()
with open('ign.csv', 'r') as f:
    next(f)
    reader = csv.reader(f)
    mogrified = [
        cur.mogrify("(%s, %s, %s, %s, %s, %s, %s, %s, %s)", row).decode('utf-8') for row in reader
    ]
mogrified_values = ",".join(mogrified)
cur.execute('insert into ign_reviews values' + mogrified_values)
conn.commit()

# use copy expert with csv header option
conn = psycopg2.connect("dbname=dbname user=user")
cur = conn.cursor()
with open('ign.csv', 'r') as f:
    cur.copy_expert('copy ign_reviews from stdin csv header', f)
conn.commit()

# copy database to python object
conn = psycopg2.connect("dbname=dbname user=user")
cur = conn.cursor()
with open('old_ign_reviews.csv', 'w') as f:
    cur.copy_expert('copy old_ign_reviews to stdout with csv header', f)

# insert into using selected columns
conn = psycopg2.connect("dbname=dbname user=user")
cur = conn.cursor()
cur.execute("""
INSERT INTO ign_reviews (
    id, score_phrase, title, url, platform, score,
    genre, editors_choice, release_date
)
SELECT id, score_phrase, title_of_game_review as title,
    url, platform, score, genre, editors_choice,
    to_date(release_day || '-' || release_month || '-' || release_year, 'DD-MM-YYYY') as release_date
FROM old_ign_reviews
""")
conn.commit()
