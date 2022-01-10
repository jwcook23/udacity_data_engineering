import configparser
import psycopg2
from sql_queries import create_syntax, drop_syntax


def drop_tables(cur, conn):
    drop = drop_syntax()
    print('Dropping tables if they exist:\n{tables}.'.format(tables=list(drop.keys())))
    for table, query in drop.items():
        query = query.format(table=table)
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    create = create_syntax()
    print('Creating tables:\n{tables}.'.format(tables=list(create.keys())))
    for table, query in create.items():
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