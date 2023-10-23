import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries

# copy from json files to staging tables
def load_staging_tables(cur, conn):
    print(f'loading staging tables')
    for query in copy_table_queries:
        print(f'doing query {query}')
        cur.execute(query)
        print('committing')
        conn.commit()

# insert from staging tables to dim/fact tables
def insert_tables(cur, conn):
    print('inserting tables')
    for query in insert_table_queries:
        print(f'doing query {query}')
        cur.execute(query)
        print('committing')
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('redshift_project.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()