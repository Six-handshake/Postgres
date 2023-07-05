import psycopg2 as pg
import os
from config import *
from sshtunnel import SSHTunnelForwarder

def connect_to_db():
    connection = None
    try:
        #serv@46.48.3.74
        with SSHTunnelForwarder(
            ('46.48.3.74', 22),
            ssh_username="serv",
            ssh_password="12345678",
            remote_bind_address=('localhost', 5432)) as server:
            
            server.start()
            print('connected via ssh tunnel')

            connection = pg.connect(
                host='localhost',
                user='postgres',
                port=server.local_bind_port,
                password='postgres',
                dbname='postgres'
            )
        with connection.cursor() as cursor:
            cursor.execute(
                "SELECT version()"
            )
            print(f"{cursor.fetchall()}")

    except Exception as error:
        print(error)

    finally:
        if connection:
            connection.close()
            print("PostgreSQL connection closed")


if __name__ == "__main__":      
    connect_to_db()