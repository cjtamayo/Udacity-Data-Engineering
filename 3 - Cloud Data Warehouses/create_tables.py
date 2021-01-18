import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Dropping any existing tables, to start over
    :param cur: db cursor
    :param conn: db connection
    :return:
    """
    print('Dropping tables...')
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
        
    return


def create_tables(cur, conn):
    """
    Creating the staging and final tables (empty)
    :param cur: db cursor
    :param conn: db connection
    :return:
    """
    print('Creating tables...')
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
    return


def main():
    """
    Grabs the params from the config file, connects to RedShift, and runs the drop/create table functions
    :return:
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)
    print('Tables successfully created')

    conn.close()
    
    return


if __name__ == "__main__":
    main()