import psycopg2 as pg
import os
from dotenv import load_dotenv
from sql_executor import SqlExecutor
from path_finder import find_paths


load_dotenv()
HOST = os.getenv('HOST')
USER = os.getenv('USER')
PORT = os.getenv('PORT')
PASSWORD = os.getenv('PASSWORD')
DB_NAME = os.getenv('DB_NAME')
TABLE_NAME = os.getenv('TABLE_NAME')


def get_links(le1: str, le2: str):
    conn = connect_to_db()

    executor = SqlExecutor(conn, execute, TABLE_NAME)

    data = find_paths(executor, ['child', 'parent', 'kind', 'date_begin', 'date_end', 'share'], le1, le2)

    close_connection(conn)

    return data


def connect_to_db():
    connection = None
    try:
        connection = pg.connect(
            host=HOST,
            user=USER,
            port=PORT,
            password=PASSWORD,
            dbname=DB_NAME
        )
    except Exception as error:
        print(error)
        return None
    finally:
        print('connected to database')
        return connection


def close_connection(conn):
    print('connection closed')
    conn.close()


def execute(conn, query):
    """
    execute sql query
    :param conn: connection
    :param query: sql query
    :return: query execution results
    """
    if conn is None:
        return None

    try:
        with conn.cursor() as cursor:
            cursor.execute(query)
            res = cursor.fetchall()
            return res
    except Exception as e:
        return None



