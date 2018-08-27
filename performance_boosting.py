# use index
conn = psycopg2.connect(dbname="dbname", user="user", password="abc123")
cur = conn.cursor()
cur.execute("CREATE INDEX state_idx ON homeless_by_coc(state)")
conn.commit()
cur.execute("EXPLAIN (ANALYZE, format json) SELECT * FROM homeless_by_coc WHERE state='CA'")
pp.pprint(cur.fetchall())

# drop index and compare performance
conn = psycopg2.connect(dbname="dbname", user="user", password="abc123")
cur = conn.cursor()
cur.execute("CREATE INDEX state_idx ON homeless_by_coc(state)")
conn.commit()
cur.execute("EXPLAIN (ANALYZE, format json) SELECT * FROM homeless_by_coc WHERE state='CA'")
pp.pprint(cur.fetchall())
cur.execute("DROP INDEX state_idx")
conn.commit()
cur.execute("EXPLAIN (ANALYZE, format json) SELECT * FROM homeless_by_coc WHERE state='CA'")
pp.pprint(cur.fetchall())

# index on join operation
conn = psycopg2.connect(dbname="dbname", user="user", password="abc123")
cur = conn.cursor()
#cur.execute("CREATE INDEX state_idx ON homeless_by_coc(state)")
#conn.commit()
cur.execute("CREATE INDEX state_idx ON homeless_by_coc(state)")
conn.commit()
cur.execute("EXPLAIN ANALYZE SELECT hbc.state, hbc.coc_number, hbc.coc_name, si.name FROM homeless_by_coc as hbc, state_info as si WHERE hbc.state = si.postal")
pp.pprint(cur.fetchall())
cur.execute("DROP INDEX IF EXISTS state_idx")
conn.commit()
cur.execute("EXPLAIN ANALYZE SELECT hbc.state, hbc.coc_number, hbc.coc_name, si.name FROM homeless_by_coc as hbc, state_info as si WHERE hbc.state = si.postal")
pp.pprint(cur.fetchall())

# create multi-column index
conn = psycopg2.connect(dbname="dbname", user="user", password="abc123")
cur = conn.cursor()
#cur.execute("CREATE INDEX state_idx ON homeless_by_coc(state)")
cur.execute("CREATE INDEX state_idx ON homeless_by_coc(state)")
conn.commit()
cur.execute("EXPLAIN ANALYZE SELECT * FROM homeless_by_coc WHERE state='CA' AND year > '1991-01-1'")
pp.pprint(cur.fetchall())
cur.execute("DROP INDEX IF EXISTS state_idx")
cur.execute("CREATE INDEX state_year_idx ON homeless_by_coc(state, year)")
conn.commit()
cur.execute("EXPLAIN ANALYZE SELECT * FROM homeless_by_coc WHERE state='CA' AND year > '1991-01-1'")
pp.pprint(cur.fetchall())

# another example
conn = psycopg2.connect(dbname="dbname", user="user", password="abc123")
cur = conn.cursor()
cur.execute("CREATE INDEX state_year_coc_number_idx ON homeless_by_coc(state,year, coc_number)")
conn.commit()

# options in creating index
conn = psycopg2.connect(dbname="dbname", user="user", password="abc123")
cur = conn.cursor()
cur.execute("CREATE INDEX state_year_idx ON homeless_by_coc(state, year ASC)")
conn.commit()
cur.execute("SELECT DISTINCT year FROM homeless_by_coc WHERE state='CA' AND year > '1991-01-01'")
ordered_years = cur.fetchall()
pp.pprint(ordered_years)

# use expression when creating index
conn = psycopg2.connect(dbname="dbname", user="user", password="abc123")
cur = conn.cursor()
cur.execute("CREATE INDEX measures_idx ON homeless_by_coc(lower(measures))")
conn.commit()
cur.execute("SELECT * FROM homeless_by_coc WHERE lower(measures)='unsheltered homeless people in families'")
unsheltered_row = cur.fetchone()

# partial indexing
conn = psycopg2.connect(dbname="dbname", user="user", password="abc123")
cur = conn.cursor()
cur.execute("CREATE INDEX state_count_idx ON homeless_by_coc(state) WHERE count > 0")
conn.commit()
cur.execute("EXPLAIN ANALYZE SELECT * FROM homeless_by_coc WHERE state='CA' AND count > 0")
pp.pprint(cur.fetchall())

# another example
conn = psycopg2.connect(dbname="dbname", user="user", password="abc123")
cur = conn.cursor()
cur.execute("CREATE INDEX state_year_measures_idx ON homeless_by_coc(state, lower(measures)) WHERE year > '2007-01-01'")
conn.commit()
cur.execute("""
EXPLAIN ANALYZE SELECT hbc.year, si.name, hbc.count
FROM homeless_by_coc hbc, state_info si WHERE hbc.state = si.postal
AND hbc.year > '2007-01-01' AND hbc.measures != 'total homeless'
""")
pp.pprint(cur.fetchall())

# count number of dead rows
conn = psycopg2.connect(dbname="dbname", user="user", password="abc123")
cur = conn.cursor()
cur.execute("SELECT n_dead_tup FROM pg_stat_all_tables WHERE relname='homeless_by_coc'")
print(cur.fetchone()[0])
cur.execute("DELETE FROM homeless_by_coc")
with open('homeless_by_coc.csv') as f:
    cur.copy_expert('COPY homeless_by_coc FROM STDIN WITH CSV HEADER', f)
conn.commit()
cur.execute("SELECT n_dead_tup FROM pg_stat_all_tables WHERE relname='homeless_by_coc'")
homeless_dead_rows = cur.fetchone()[0]

# disabling transaction by using autocommit
conn = psycopg2.connect(dbname="dbname", user="user", password="abc123")
conn.autocommit = True
cur = conn.cursor()
cur.execute("SELECT n_dead_tup FROM pg_stat_all_tables WHERE relname='homeless_by_coc'")
print(cur.fetchone()[0])
cur.execute("VACUUM homeless_by_coc")
cur.execute("SELECT n_dead_tup FROM pg_stat_all_tables WHERE relname='homeless_by_coc'")
homeless_dead_rows = cur.fetchone()[0]

# vaccum full option
conn = psycopg2.connect(dbname="dbname", user="user", password="abc123")
conn.autocommit = True
cur = conn.cursor()
cur.execute("VACUUM FULL")
