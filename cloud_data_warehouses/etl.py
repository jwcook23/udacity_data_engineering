import configparser
from collections import defaultdict
import psycopg2
from sql_queries import copy, insert


def load_staging_tables(cur, conn):
    for table_s3, query in copy.items():

        table = table_s3[0]
        bucket = table_s3[1]
        print(f'Beginning copy from S3 bucket {bucket} into table {table}.')

        starttime = stl_load_starttime(cur, bucket)

        query = query(table=table, bucket=bucket)
        cur.execute(query)
        conn.commit()
        print(f'Completed copy from S3 bucket {bucket} into table {table}.')

        load_errors = stl_load_errors(cur, bucket, starttime)
        if load_errors:
            print(f'Errors skipped during copy:\n{load_errors}')


def insert_tables(cur, conn):
    for table, query in insert.items():
        query = query.format(table=table)
        print(f'Inserting data into table {table}.')
        cur.execute(query)
        conn.commit()

def stl_load_starttime(cur, bucket):

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
    starttime = cur.fetchone()
    starttime = starttime[0]
    if starttime is not None:
        starttime = str(starttime)

    return starttime

def stl_load_errors(cur, bucket, starttime):
    ''' Get latest Redshift error detail from the table stl_load_errors 
    if it is referred to in err, then raise a RedshiftLoadError exception.
    Otherwise err is re-raised.

    A RedshiftLoadError exeception describes err_reason and the source 
    filename for the error. Keys represent Redshift table column names.

    There is no guarantee the error comes from the table being loaded
    into. See https://forums.aws.amazon.com/thread.jspa?messageID=897976 for
    explanation that the table id in stl_load_errors doesn't reference a 
    physical table.

    Parameters
    ----------
    cur (psycopg2.connect.cursor) : cursor for executing statements
    err (Exception) : any exception that is raised

    Returns
    -------
    load_errors (dict) : 
    '''

    # use wildcard search in case nested folders are loaded
    bucket = bucket[0:-1]+"%"+bucket[-1]

    # get new errors since last load
    if starttime is None:
        starttime = ""
    else:
        starttime = f"AND starttime > '{starttime}'"
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

    return load_errors

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