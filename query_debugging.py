# use explain
import pprint as pp
conn = psycopg2.connect(dbname="dbname", user="user", password="abc123")
cur = conn.cursor()
cur.execute("EXPLAIN SELECT * FROM tablename")
pp.pprint(cur.fetchall())

# use explain
conn = psycopg2.connect(dbname="dbname", user="user", password="abc123")
cur = conn.cursor()
cur.execute("EXPLAIN SELECT COUNT(*) FROM homeless_by_coc WHERE year > '2012-01-01'")
pp.pprint(cur.fetchall())

# format option for explain
conn = psycopg2.connect(dbname="dbname", user="user", password="abc123")
cur = conn.cursor()
cur.execute("EXPLAIN (format json) SELECT COUNT(*) FROM homeless_by_coc WHERE year > '2012-01-01'")
pp.pprint(cur.fetchall())

# use analyze option in explain
conn = psycopg2.connect(dbname="dbname", user="user", password="abc123")
cur = conn.cursor()
cur.execute("EXPLAIN (ANALYZE, FORMAT json) SELECT COUNT(*) FROM homeless_by_coc WHERE year > '2012-01-01'")
pp.pprint(cur.fetchall())

# use roolback to revert changes
conn = psycopg2.connect(dbname="dbname", user="user", password="abc123")
cur = conn.cursor()
cur.execute("EXPLAIN (ANALYZE, FORMAT json) DELETE FROM state_household_incomes")
conn.rollback()
pp.pprint(cur.fetchall())

# explain join query
conn = psycopg2.connect(dbname="dbname", user="user", password="abc123")
cur = conn.cursor()
cur.execute("EXPLAIN (ANALYZE, FORMAT json) SELECT hbc.state, hbc.coc_number, hbc.coc_name, si.name FROM homeless_by_coc as hbc, state_info as si WHERE hbc.state = si.postal")
pp.pprint(cur.fetchall())

#
