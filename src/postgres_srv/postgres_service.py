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
        with connection.cursor() as cursor:
            cursor.execute(
                'SELECT version()'
            )
            print(f"Server version:{cursor.fetchone()}")
    except Exception as error:
        print(error)
    finally:
        if connection:
            connection.close()
            print("PostgreSQL connection closed")
            
