'''Explorary data analysis to determine null-able columns.'''

import psycopg2
import pandas as pd

conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
cur = conn.cursor()

# for each table, determine columns without null values
tables = ['artists','songplays','songs','time','users']
for t in tables:

    # select all values into a dataframe
    cur.execute(f'SELECT * FROM {t}')
    columns = [x.name for x in cur.description]
    result = cur.fetchall()
    df = pd.DataFrame(result, columns=columns)

    # determine columsn without nulls
    not_null = df.notna().all()
    not_null = list(not_null[not_null].index)

    # output result for each table
    print(f'table: {t} not_null columns: {not_null}')