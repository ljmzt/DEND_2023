# for n=1e+7
# inserting into table without primary key takes about 12 sec
# inserting into table with ''' takes about 63 sec
# join into the table without primary key takes about 3.4 sec
# ''' with primary key takes < 0.1 sec
import random
import psycopg2
from datetime import datetime
import os

random.seed(1)
n = int(1e+7)

# create the input data
x = []
for i in range(n):
    x.append((i, random.random()))
random.shuffle(x)
with open('tmp_input.txt','w') as fid:
    for id, val in x:
        fid.write(f"{str(id)}|{str(val)}\n")

# create connection
conn = psycopg2.connect('postgresql://zma@localhost/student_db')
conn.set_session(autocommit=True)
cur = conn.cursor()

# insert into table without primary key
print('for table without primary key')
cur.execute('DROP TABLE IF EXISTS large_no_primary_key')
cur.execute('CREATE TABLE large_no_primary_key (id int, val float)')
print(f"start inserting {datetime.now()}")
cur.execute('''
COPY large_no_primary_key 
FROM '/Users/zma/Documents/DEND-2023.dir/learn_sql.dir/tmp_input.txt'
DELIMITER '|'
''')
print(f"end inserting {datetime.now()}")

# insert into table with primary key
print('for table with primary key')
cur.execute('DROP TABLE IF EXISTS large_with_primary_key')
cur.execute('CREATE TABLE large_with_primary_key (id int PRIMARY KEY, val float)')
print(f"start inserting {datetime.now()}")
cur.execute('''
COPY large_with_primary_key 
FROM '/Users/zma/Documents/DEND-2023.dir/learn_sql.dir/tmp_input.txt'
DELIMITER '|'
''')
print(f"end inserting {datetime.now()}")

# now create a short table to test joint
try:
    cur.execute('DROP TABLE short')
except:
    pass
cur.execute('CREATE TABLE short (id int)')
for _ in range(int(1e+4)):
    cur.execute('INSERT INTO short VALUES (%s)', [random.randint(1,1e+5)])

# test some join
print(' ==== no primary key ==== ')
print(f'starting join {datetime.now()}')
cur.execute('''
SELECT l.id, l.val
  FROM short s JOIN large_no_primary_key l
  ON s.id = l.id
  ORDER BY s.id;
''')
rows = cur.fetchall()
print(f'ending join {datetime.now()}')
print(rows[:5]) 

print(' ==== with primary key ==== ')
print(f'starting join {datetime.now()}')
cur.execute('''
SELECT l.id, l.val
  FROM short s JOIN large_with_primary_key l
  ON s.id = l.id
  ORDER BY s.id;
''')
rows = cur.fetchall()
print(f'ending join {datetime.now()}')
print(rows[:5])

print(' ==== no primary key ==== ')
print(f'starting join {datetime.now()}')
cur.execute('''
SELECT l.id, l.val
  FROM short s JOIN large_no_primary_key l
  ON s.id = l.id
  ORDER BY s.id;
''')
rows = cur.fetchall()
print(f'ending join {datetime.now()}')
print(rows[:5]) 

print(' ==== with primary key ==== ')
print(f'starting join {datetime.now()}')
cur.execute('''
SELECT l.id, l.val
  FROM short s JOIN large_with_primary_key l
  ON s.id = l.id
  ORDER BY s.id;
''')
rows = cur.fetchall()
print(f'ending join {datetime.now()}')
print(rows[:5])

# clean up
os.remove('tmp_input.txt')
cur.execute('DROP TABLE large_with_primary_key')
cur.execute('DROP TABLE large_no_primary_key')