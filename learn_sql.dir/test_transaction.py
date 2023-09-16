import psycopg2
import asyncio
import time

async def alter(sleep_sec=1.0):
    print(f'here {sleep_sec}')
    try:
        conn = psycopg2.connect('postgres://student:student@localhost/student_db')
        conn.set_session(autocommit=False)
        cur = conn.cursor()
        # it actually hangs if I put the await between the two update!!
        await asyncio.sleep(sleep_sec)
        cur.execute("UPDATE accounts SET balance = balance - 50 WHERE name = 'Alice'")
        cur.execute("UPDATE accounts SET balance = balance + 50 WHERE name = 'Bob'")
        conn.commit()
    except psycopg2.DatabaseError as e:
        print(f"fail for sleep_sec={sleep_sec}")
        print(e)
    finally:
        conn.close()
    print(f'here {sleep_sec}')

async def main():
    await asyncio.gather(alter(5.0), alter(2.0))
    # await asyncio.gather(alter(5.0))

if (__name__ == '__main__'):

    # prepare the table
    conn = psycopg2.connect('postgres://student:student@localhost/student_db')
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    print('before:')
    cur.execute('DROP TABLE IF EXISTS accounts')
    cur.execute('CREATE TABLE accounts (name text, balance real CHECK (balance >= 0))')
    cur.execute('INSERT INTO accounts VALUES (%s, %s)', ('Alice',80))
    cur.execute('INSERT INTO accounts VALUES (%s, %s)', ('Bob',0))
    cur.execute('SELECT * FROM accounts')
    rows = cur.fetchall()
    print(rows)
    conn.close()

    # use asyncio to update the table
    t1 = time.perf_counter()
    asyncio.run(main())
    t2 = time.perf_counter()
    print(f"elapsed time: {t2-t1}")

    # check whether I finish the transaction
    print('after:')
    conn = psycopg2.connect('postgres://student:student@localhost/student_db')
    conn.set_session(autocommit=True)
    cur = conn.cursor()
    cur.execute('SELECT * FROM accounts')
    rows = cur.fetchall()
    print(rows)
    conn.close()
