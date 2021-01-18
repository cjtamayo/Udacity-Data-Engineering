import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Filling the staging/raw tables with data from S3
    :param cur: db cursor
    :param conn: db connection
    :return:
    """
    print('Loading staging tables')
    for query in copy_table_queries:
        print("Loading table '{}'".format(query.split()[1]))
        cur.execute(query)
        conn.commit()
        
    return


def insert_tables(cur, conn):
    """
    Filling the final tables from the raw redshift tables
    :param cur: db cursor
    :param conn: db connection
    :return:
    """
    print('Inserting data')
    for query in insert_table_queries:
        print("Loading table '{}'".format(query.split()[2]))
        cur.execute(query)
        conn.commit()
        
    return


def main():
    """
    Grabs the params from the config file, connects to RedShift, and runs the insert table functions
    :return:
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()
    
    return


if __name__ == "__main__":
    main()