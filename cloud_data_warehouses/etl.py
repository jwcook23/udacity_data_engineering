import configparser
from collections import defaultdict
import re
import psycopg2
from sql_queries import copy, insert


def load_staging_tables(cur, conn):
    for table_s3, query in copy.items():
        table = table_s3[0]
        s3 = table_s3[1]
        print(f'Beginning copy from S3 bucket {s3} into table {table}.')
        query = query(table=table, s3=s3)
        try:
            cur.execute(query)
        except Exception as err:
            redshift_error_detail(cur, err)
        conn.commit()
        print(f'Completed copy from S3 bucket {s3} into table {table}.')


def insert_tables(cur, conn):
    for table, query in insert:
        print(f'Inserting data into table {table}.')
        cur.execute(query)
        conn.commit()

class RedShiftLoadError(Exception):
    ''' Error when loading data into a Redshift table.'''
    pass

def redshift_error_detail(cur, err):
    ''' Get latest Redshift error detail from the table stl_load_errors 
    if it is referred to in err, then raise a RedshiftLoadError exception.
    Otherwise err is re-raised.

    A RedshiftLoadError exeception describes unique errors for each column.
    Keys represent column names and values are a unique list of errors for 
    each column.

    Note there is no guarantee the error comes from the table being loaded
    into. See https://forums.aws.amazon.com/thread.jspa?messageID=897976 for
    explanation that the table id in stl_load_errors doesn't reference a 
    physical table.

    Parameters
    ----------
    cur (psycopg2.connect.cursor) : cursor for executing statements
    err (Exception) : any exception that is raised

    Returns
    -------
    None
    '''
    cur.connection.rollback()
    if "Check 'stl_load_errors' system table for details" in str(err):
        cur.execute(f'''
            SELECT colname, err_reason, tbl
            FROM stl_load_errors
            WHERE starttime = (
                SELECT MAX(starttime) from stl_load_errors
            )
        ''')
        error = cur.fetchall()
        parsed = defaultdict(set)
        for err in error:
            colname = err[0].strip()
            err_reason = err[1].strip()
            parsed[colname].add(err_reason)
        parsed = dict(parsed)
        raise RedShiftLoadError(parsed)
    else:
        raise

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