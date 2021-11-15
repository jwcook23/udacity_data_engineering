import configparser
import psycopg2
from sql_queries import copy, insert


def load_staging_tables(cur, conn):
    for table_s3, query in copy.items():
        table = table_s3[0]
        s3 = table_s3[1]
        print(f'Copying data from S3 bucket {s3} into table {table}.')
        query = query(table=table, s3=s3)
        cur.execute(query)
        # TODO: some columns fail to load, probably from nulls
        # conn.rollback()
        # cur.execute('select * from stl_load_errors;')
        # err = cur.fetchall()
        conn.commit()


def insert_tables(cur, conn):
    for table, query in insert:
        print(f'Inserting data into table {table}.')
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read('dwh.cfg')
    config = dict(config.items('CLUSTER'))

    conn = psycopg2.connect(f"""
        host={config['HOST']} dbname={config['DB_NAME']} 
        user={config['DB_USER']} password={config['DB_PASSWORD']} 
        port={config['DB_PORT']}"""
    )
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    # insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()