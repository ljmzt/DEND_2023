import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

# drop table for reproducing results
def drop_tables(cur, conn):
    print('droping tables')
    for query in drop_table_queries:
        print(f'doing query {query}')
        cur.execute(query)
        conn.commit()

# create table
def create_tables(cur, conn):
    print('create tables')
    for query in create_table_queries:
        print(f'doing query {query}')
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('redshift_project.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()