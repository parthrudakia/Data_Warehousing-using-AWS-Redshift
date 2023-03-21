import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Copies S3 data to staging tables on Redshift
    
    Takes cursor and connection as input
    
    """
    
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts staging table data into analytic tables on Redshift
    
    Takes cursor and connection as input
    
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    """
    Establishes connection to Redshift database using params from dwh.cfg
    Calls functions to load data from S3 to staging tables on Redshift
    Calls functions to insert data from staging tables to analytic tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    

    conn=psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()