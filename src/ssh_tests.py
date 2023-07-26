import sqlalchemy.orm
from sshtunnel import SSHTunnelForwarder
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import text
import os
from dotenv import load_dotenv
from sql_executor import SqlExecutor
from path_finder import find_paths, find_all_links


load_dotenv()
HOST = os.getenv('HOST')
IP_ADDRESS = os.getenv('IP_ADDRESS')
SSH_PORT = int(os.getenv('PORT'))
SSH_USERNAME = os.getenv('SSH_USERNAME')
SSH_PASSWORD = os.getenv('SSH_PASSWORD')
TABLE_NAME = os.getenv('TABLE_NAME')


def get_links_via_ssh(le1: str, le2: str):
    """
    get links between two legal entities
    :param le1: first legal entity
    :param le2: second legal entity
    :return: list of legal entities and individuals
    """
    with get_ssh_server() as server:
        server.start()
        print('Server connected via SSH')

        session = connect_to_db(str(server.local_bind_port))

        executor = SqlExecutor(session, execute, TABLE_NAME)

        result = find_paths(executor, ['child', 'parent', 'kind', 'date_begin', 'date_end', 'share'], le1, le2)

        close_connection_to_db(session)

        return result


def get_all_links_via_ssh(le: str):
    """
    get links between two legal entities
    :param le: legal entity
    :return: list of legal entities and individuals
    """
    with get_ssh_server() as server:
        server.start()
        print('Server connected via SSH')

        session = connect_to_db(str(server.local_bind_port))

        executor = SqlExecutor(session, execute, TABLE_NAME)

        result = find_all_links(executor, ['child', 'parent', 'kind', 'date_begin', 'date_end', 'share'], le)

        close_connection_to_db(session)

        return result


def get_ssh_server() -> SSHTunnelForwarder:
    return SSHTunnelForwarder(
        (IP_ADDRESS, 22),
        ssh_username=SSH_USERNAME,
        ssh_password=SSH_PASSWORD,
        remote_bind_address=(HOST, SSH_PORT))


def connect_to_db(local_port: str) -> sqlalchemy.orm.Session:
    engine = create_engine('postgresql://postgres:postgres@127.0.0.1:' + local_port + '/postgres')

    session_maker = sessionmaker(bind=engine)
    session = session_maker()

    print('Database session created')

    return session


def close_connection_to_db(sess: sqlalchemy.orm.Session):
    print('Database session closed')
    sess.close()


def execute(sess: sqlalchemy.orm.Session, query: str):
    """
    execute sql query
    :param sess: session
    :param query: sql query
    :return: query execution results
    """
    if sess is None:
        return None

    try:
        result = sess.execute(text(query)).fetchall()
        return list(result)
    except Exception as e:
        print(e)
        return None
