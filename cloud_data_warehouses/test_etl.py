'''Ensure primary key values are made unique during the ETL process from
the staging tables to the fact and dimension tables since Redshift does
not enforce primary key uniqueness.'''

import configparser
from numpy import character
import psycopg2
import pandas as pd
import warnings

class PossibleCopyStringTruncation(Warning):
    '''Warn for strings that were possibly truncated during the copy from S3 to Redshift.'''
    pass

def get_schema(conn, db_name):
    '''Get table schema from Redshift to perform checks against.'''

    # get schema of each table, excluding columns internal to Redshift
    query = f"""
        SELECT 
            table_name,
            column_name,
            character_maximum_length,
            remarks
        FROM
            SVV_COLUMNS 
        WHERE table_catalog = '{db_name}'
        AND table_schema = 'public'
    """
    schema = pd.read_sql_query(query, conn, dtype={'character_maximum_length': 'Int64'})

    # staging tables
    schema_staging = schema[schema['table_name'].str.startswith('staging')]
    # non-staging tables
    schema_main = schema[~schema['table_name'].str.startswith('staging')]
    
    # check additional 'PRIMARY KEY' remark for non-staging tables to prevent need of complicated join
    pks = schema_main[schema_main['remarks']=='PRIMARY KEY']['table_name']
    pks = schema_main['table_name'].drop_duplicates()
    undefined = ~pks.isin(schema_main[schema_main['remarks']=='PRIMARY KEY']['table_name'])
    if any(undefined):
        raise AttributeError(f'PRIMARY KEY comment for table(s) {pks[undefined]} were not added during table creation.')
    
    return schema_staging, schema_main

def check_contents(schema_staging, schema_main, conn):
    '''For non-staging tables, verify the uniqueness of primary keys and identify the size
    of each table. For staging tables, identify columns possibly truncated during Redshift copy.'''

    # staging tables, identify values possibly truncated during Copy
    strings = schema_staging.loc[
        schema_staging['character_maximum_length'].notna(),
        ['table_name','column_name','character_maximum_length']
    ]
    strings = strings.reset_index(drop=True)
    strings['character_maximum_length'] = strings['character_maximum_length'].astype('str')
    base = '''
    SELECT
        '{table}' AS table_name,
        '{column}' AS column_name,
        SUM(CASE WHEN LEN({column})={len} THEN 1 ELSE 0 END)/CAST(COUNT({column}) AS FLOAT)*100 AS col_percent
    FROM {table}
    HAVING col_percent>0
    '''
    query = []
    for _,row in strings.iterrows():
        query+= [base.format(table=row['table_name'], column=row['column_name'], len=row['character_maximum_length'])]
    query = '\nUNION\n'.join(query)
    truncated = pd.read_sql_query(query, conn)
    if len(truncated)>0:
        msg = '''Possible truncated string values during copy from S3 to Redshift.
        0>col_percent<100% has truncated records.
        col_percent=100% is possibly truncated (all values may be equal to the max column length).
        '''
        msg += truncated.to_string()
        warnings.warn(msg,PossibleCopyStringTruncation)   

    # non-staging, verify primary key uniqueness and show size of tables
    pks = schema_main.loc[schema_main['remarks']=='PRIMARY KEY',['table_name','column_name']]
    pks = pks.set_index(keys='table_name')
    pks = pd.Series(pks['column_name'], index=pks.index)
    pks = pks.to_dict()
    base = """
    SELECT 
        '{table_name}' AS table_name,
        COUNT({pk_column})-COUNT(DISTINCT({pk_column})) AS pks_duplicated,
        COUNT({pk_column}) AS length
    FROM 
        {table_name}
    """
    query = [base.format(table_name=k,pk_column=v) for k,v in pks.items()]
    query = "\nUNION\n".join(query)
    contents = pd.read_sql_query(query, conn, index_col='table_name')
    duplicated = list(contents[contents>0].index)
    if any(duplicated):
        raise AttributeError(f'Table(s) {duplicated} contain duplicated primary key values.')
    print('Record count for each non-staging table: {size}'.format(size=contents['length'].to_dict()))

if __name__ == "__main__":
    
    # connect to database
    config = configparser.ConfigParser()
    config.optionxform = str
    config.read('dwh.cfg')
    config = dict(config.items('CLUSTER'))

    conn = psycopg2.connect(f"""
        host={config['HOST']} dbname={config['DB_NAME']} 
        user={config['DB_USER']} password={config['DB_PASSWORD']} 
        port={config['DB_PORT']}"""
    )

    schema_staging, schema_main = get_schema(conn, db_name=config['DB_NAME'])
    check_contents(schema_staging, schema_main, conn)