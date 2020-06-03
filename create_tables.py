import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop all tables from the Sparkify database on AWS
    """
    for idx, query in enumerate(drop_table_queries):
        try:
            cur.execute(query)
            conn.commit()
            print("Success: Dropping Table {}".format(idx))
        except psycopg2.Error as e:
            print("Error: Dropping Table {}".format(idx))
            print (e)

def create_tables(cur, conn):
    """
    Create all tables on the Sparkify database on AWS
    """
    for idx, query in enumerate(create_table_queries):
        try:
            cur.execute(query)
            conn.commit()
            print("Success: Creating Table {}".format(idx))
        except psycopg2.Error as e:
            print("Error: Creating Table {}".format(idx))
            print (e)


def main():
    """
    Connect to the Sparkify database on AWS to drop and connect tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()