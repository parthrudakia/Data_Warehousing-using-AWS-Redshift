import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops all tables if any exists in redshift for reuse purpose
    
    Takes cursor and connection as input
    
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    creates staging tables and analytics tables 
    
    Takes cursor and connection as input
    
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    - Drops (if exists) and Creates the AWS Redshift Database
    
    - Establishes connection with the redshift database and set cursor to it.  
    
    - Drops all the tables.  
    
    - Creates all tables. 
    
    - Closes the connection at last. 
    
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