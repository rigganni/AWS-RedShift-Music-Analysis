import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    DWH_HOST = config.get("DWH", "DWH_HOST")
    DWH_DB= config.get("DWH","DWH_DB")
    DWH_DB_USER= config.get("DWH","DWH_DB_USER")
    DWH_DB_PASSWORD= config.get("DWH","DWH_DB_PASSWORD")
    DWH_PORT = config.get("DWH","DWH_PORT")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(DWH_HOST, DWH_DB, DWH_DB_USER, DWH_DB_PASSWORD, DWH_PORT))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()