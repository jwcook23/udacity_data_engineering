import configparser
from collections import defaultdict
import json
import time
import psycopg2
from sql_queries import copy_syntax, insert_syntax


def load_staging_tables(cur, conn):

    copy = copy_syntax()
    for table_s3, query in copy.items():

        table = table_s3[0]
        bucket = table_s3[1]
        print(f'Beginning copy from S3 bucket {bucket} into table {table}.')

        load_start = stl_load_starttime(cur, bucket)

        query = query(table=table, bucket=bucket)
        time_start = time.time()
        cur.execute(query)
        conn.commit()
        print(f'Completed copy from S3 bucket {bucket} into table {table} in {time.time()-time_start:.2f} seconds.')

        stl_load_errors(cur, bucket, table, load_start)


def insert_tables(cur, conn):
    insert = insert_syntax()
    for table, query in insert.items():
        query = query.format(table=table)
        print(f'Inserting data into table {table}.')
        cur.execute(query)
        conn.commit()

def stl_load_starttime(cur, bucket):
    '''Determine timestamp of previous load error from a S3 bucket. Used to determine any new errors that occur.
    
    Parameters
    ----------
    cur (psycopg2.connect.cursor) : cursor for execute SQL statements
    bucket (str) : url path to S3 bucket such as 's3://udacity-dend/song-data/A'

    Returns
    -------
    previous_error_ts (str) : starttime of previous load errors in 'YYYY-MM-DD hh:mm:ss.fff' format

    '''

    # use wildcard search in case nested folders are loaded
    bucket = bucket[0:-1]+"%"+bucket[-1]

    # find last load time for a bucket
    query = f"""
    SELECT 
        MAX(starttime)
    FROM
        stl_load_errors 
    WHERE filename LIKE {bucket}
    """
    cur.execute(query)
    previous_error_ts = cur.fetchone()
    previous_error_ts = previous_error_ts[0]
    if previous_error_ts is not None:
        previous_error_ts = str(previous_error_ts)

    return previous_error_ts

def stl_load_errors(cur, bucket, table, previous_error_ts):
    ''' Determine S3 files that were not loaded into a staging table during a COPY
    command due to the COPY MAXERROR parameter. If errors are encountered, they
    are written to a file named 'stl_load_errors_{table}.json'.

    Parameters
    ----------
    cur (psycopg2.connect.cursor) : cursor for executing SQL statements
    bucket (str) : url path to S3 bucket such as 's3://udacity-dend/song-data/A'
    table (str) : name of Redshift database table
    previous_error_ts (str) : starttime of previous load errors in 'YYYY-MM-DD hh:mm:ss.fff' format

    Returns
    -------
    None

    The file named 'stl_load_errors_{table}.json' contains error reasons and source S3 file names for
    each column that resulted in an error in the following format.

    {
        columnA: {'err_reason': ['SomeErrorReason'], 'filename': ['s3://Bucket1']},
        columnB: {'err_reason': ['SomeErrorReason'], 'filename': ['s3://Bucket2']}
    }

    '''

    # use wildcard search in case nested folders are loaded
    bucket = bucket[0:-1]+"%"+bucket[-1]

    # get new errors since last load
    if previous_error_ts is None:
        starttime = ""
    else:
        starttime = f"AND starttime > '{previous_error_ts}'"
    cur.execute(f"""
        SELECT filename, colname, err_reason
        FROM stl_load_errors
        WHERE filename LIKE {bucket}
        {starttime}
    """)
    error = cur.fetchall()

    # list error reasons and source file names for columns causing errors
    load_errors = defaultdict(list)
    for err in error:
        filename = err[0].strip()
        colname = err[1].strip()
        err_reason = err[2].strip()
        load_errors[colname].append({'err_reason': err_reason, 'filename': filename})
    load_errors = dict(load_errors)

    if load_errors:
        error_file = f'stl_load_errors_{table}.json'
        print(f'Saving table {table} load errors into file {error_file}.')
        with open(error_file, 'w', encoding='utf-8') as fh:
            json.dump(load_errors, fh, ensure_ascii=False, indent=4)
    else:
        print(f'No errors occured loading into table {table}.')

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
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()