'''Ensure primary key values are made unique during the ETL process from
the staging tables to the fact and dimension tables since Redshift does
not enforce primary key uniqueness.'''

import configparser
import psycopg2


def check_contents(cur):

    tables = {}
    tables['users'] = {'pk': 'user_id'}
    tables['songs'] = {'pk': 'song_id'}
    tables['artists'] = {'pk': 'artist_id'}
    tables['time'] = {'pk':'start_time'}
    tables['songplay'] = {'pk':'songplay_id'}

    for t,v in tables.items():

        pk = v['pk']

        # get unique number of primary key values
        query = f"""
            SELECT 
                {pk}
            FROM 
                {t}
            GROUP BY {pk}
            HAVING COUNT({pk})>1
        """
        cur.execute(query)
        duplicates = cur.fetchall()
        duplicates = [x[0] for x in duplicates]

        # get overall number of records
        query = f'SELECT COUNT({pk}) FROM {t}'
        cur.execute(query)
        records = cur.fetchone()[0]

        # ensure primary key is unique
        if len(duplicates)==0:
            print(f'Table {t} contains {records} records and the primary key column {pk} is unique.')
        else:
            raise RuntimeError(f'Table {t} contains duplicates in the primary key column {pk}.', duplicates)

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

    check_contents(cur)

    conn.close()

if __name__ == "__main__":
    main()