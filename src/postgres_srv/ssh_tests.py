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
    session = connect_to_db()

    result = get_parents(session, le1)

    close_connection_to_db(session)

    return result


def connect_to_db() -> sqlalchemy.orm.Session:
    with SSHTunnelForwarder(
            (IP_ADDRESS, 22),
            ssh_username=SSH_USERNAME,
            ssh_password=SSH_PASSWORD,
            remote_bind_address=(HOST, SSH_PORT)) as server:
        server.start()  # start ssh sever
        print('Server connected via SSH')

        # connect to PostgreSQL
        local_port = str(server.local_bind_port)
        engine = create_engine('postgresql://postgres:postgres@127.0.0.1:' + local_port + '/postgres')

        session_maker = sessionmaker(bind=engine)
        session = session_maker()

        print('Database session created')

        print(get_parents(session, '73'))

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
