import sqlalchemy.orm
from src.postgres_srv.config import *
from sshtunnel import SSHTunnelForwarder
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import text


def get_links_via_ssh(le1: str, le2: str) -> list:
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

        result = get_parents(session, le1)

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


def execute(sess: sqlalchemy.orm.Session, query: sqlalchemy.TextClause):
    """
    execute sql query
    :param sess: session
    :param query: sql query
    :return: query execution results
    """
    if sess is None:
        return None

    try:
        result = sess.execute(query).fetchall()
        return result
    except Exception as e:
        print(e)
        return None


def get_parents(sess: sqlalchemy.orm.Session, le_id: str) -> list:
    """
    get all parents of legal entity
    :param sess: session
    :param le_id: id of legal entity
    :return: array of parent ids
    """
    sql = text(f'SELECT parent FROM "{TABLE_NAME}" WHERE child={le_id}')
    result = execute(sess, sql)
    if result is not None:
        return list(map(lambda x: x[0], result))
    return None
