# understand how to go from json to timestamp
import psycopg2
import random

n = 10

# generate repeating records
ts = []
for _ in range(n):
    ts.append(random.randint(1500000000, 1540344794))
with open('tmp_input.json','w') as fid:
    for x in ts:
        fid.write('{{"registration":{}}}\n'.format(x))

# test 1; directly convert to timestamp in copy
conn = psycopg2.connect('postgresql://student:student@localhost/student_db')
conn.set_session(autocommit=True)
cur = conn.cursor()
cur.execute('''
  DROP TABLE IF EXISTS temp;
  CREATE TABLE temp (data jsonb);
''')
cur.execute('''
  COPY temp FROM '/Users/zma/Documents/DEND-2023.dir/learn_sql.dir/tmp_input.json'
''')
cur.execute('''
  DROP TABLE IF EXISTS test1;
  CREATE TABLE test1 (ts timestamp);
''')
cur.execute('''
  INSERT INTO test1 (ts)
  SELECT TO_TIMESTAMP((data->>'registration')::INTEGER)
  FROM temp
''')
cur.execute('SELECT * FROM test1')
rows = cur.fetchall()
print(rows)