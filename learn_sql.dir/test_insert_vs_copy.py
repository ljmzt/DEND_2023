# login as superuser to test copy
# this code is to demonstrate it's much better to use copy instead of insert into if file is large
from datetime import datetime
import psycopg2
import random
import os

# create the test value
random.seed(1)
n = int(1e+5)
x = []
for i in range(n):
    x.append((i, random.random()))
random.shuffle(x)

conn = psycopg2.connect('postgresql://student:student@localhost/student_db')
conn.set_session(autocommit=True)
cur = conn.cursor()

cur.execute('DROP TABLE IF EXISTS tmp')
cur.execute('CREATE TABLE tmp (id int, val float)')
with open('tmp_input.txt','w') as fid:
    for id, val in x:
        fid.write(f"{str(id)}|{str(val)}\n")
print(f'write to file then copy into {datetime.now()}')
cur.execute(f"COPY tmp FROM '{os.path.join(os.getcwd(), 'tmp_input.txt')}' DELIMITER '|'")
print(f'end {datetime.now()}')
cur.execute('SELECT * FROM tmp LIMIT 5')
print(cur.fetchall())

cur.execute('DROP TABLE IF EXISTS tmp')
cur.execute('CREATE TABLE tmp (id int, val float)')
print(f'insert into {datetime.now()}')
for id, val in x:
    cur.execute('INSERT INTO tmp VALUES (%s, %s)', (id, val))
print(f'end {datetime.now()}')
cur.execute('SELECT * FROM tmp LIMIT 5')
print(cur.fetchall())

# clean up
cur.execute('DROP TABLE tmp')
os.remove('tmp_input.txt')