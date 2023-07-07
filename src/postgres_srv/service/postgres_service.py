import psycopg2 as pg
from config import *


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
        return connection


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
        with conn.crusor as cursor:
            cursor.execute(
                query
            )
            res = cursor.fetchall()
            return res
    except Exception:
        return None


def get_parents(conn, le_id):
    """
    get parents of legal entity
    :param conn: connection to db
    :param le_id: id of legal entity
    :return:
    """
    return execute(conn, f'SELECT parent FROM "{TABLE_NAME}" WHERE child={le_id}')
