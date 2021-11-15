import configparser
import psycopg2
from sql_queries import create, drop


def drop_tables(cur, conn):
    for table, query in drop.items():
        print(f'Dropping table {table}.')
        query = query.format(table=table)
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    for table, query in create.items():
        print(f'Creating table {table}.')
        query = query.format(table=table)
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()