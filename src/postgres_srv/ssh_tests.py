import sqlalchemy.orm
from src.postgres_srv.config import *
from sshtunnel import SSHTunnelForwarder
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine
from sqlalchemy import text


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

        result = try_get_link(session, le1, le2)

        close_connection_to_db(session)

        return result


def try_get_link(session: sqlalchemy.orm.Session, le1: str, le2: str, depth=6):
    children = [set() for i in range(depth)]
    children[0] = {le1}
    parents = [set() for i in range(depth)]
    current_depth = 0
    while current_depth < depth and le2 not in children[current_depth]:
        parents[current_depth] = get_parents(session, children[current_depth], parents[current_depth - 1])

        if current_depth - 1 == depth:
            break

        children[current_depth + 1] = get_children(session, parents[current_depth], children[current_depth])
        current_depth += 1

    if current_depth < depth and le2 in children[current_depth]:
        return map_children(session, backtrack(session, le2, children, parents, current_depth))
    return None


def backtrack(session: sqlalchemy.orm.Session, le2: str, children: list, parents: list, depth: int):
    children[depth] = {le2}
    depth -= 1
    while depth > 0:

        parents[depth] = parents[depth].intersection(get_parents(session, children[depth + 1], parents[depth + 1]))
        children[depth] = children[depth].intersection(get_children(session, parents[depth], children[depth + 1]))

        depth -= 1
    return children


def map_children(sess: sqlalchemy.orm.Session, children_per_depth: list):
    result = []
    for depth, children in enumerate(children_per_depth):
        if len(children) == 0:
            break

        result.extend(list(map(
            lambda x: {
                "pk": x[0],
                "parent": x[1],
                "kind": x[2],
                "depth": depth
            },
            execute(sess, text(f'SELECT child, parent, kind FROM "{TABLE_NAME}" WHERE child IN {sql_subquery_transform(children)}'))
        )))
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


def sql_subquery_transform(array: set) -> str:
    """
    transform list to sql subquery
    :param array: list that should be transformed
    :return: subquery string
    """
    return '(' + ', '.join(array) + ')'


def where_in(sess: sqlalchemy.orm.Session,
             target_column: str,
             array: set,
             array_column: str,
             exclude=None,
             exclude_column=None) -> set:
    if array is None or len(array) == 0:
        return None

    sql_array = sql_subquery_transform(array)
    sql_exclude = sql_subquery_transform(exclude) if exclude is not None and len(exclude) != 0 else None

    sql = text(f'SELECT {target_column} FROM "{TABLE_NAME}" WHERE ({array_column} IN {sql_array}' +
               (f' AND {exclude_column} NOT IN {sql_exclude})' if sql_exclude is not None else ')'))

    result = execute(sess, sql)
    if result is not None:
        return set(map(lambda x: str(x[0]), result))

    return None


def get_parents(sess: sqlalchemy.orm.Session, le_ids: set, exclude=None) -> set:
    """
    get all parents of legal entity array
    :param sess: session
    :param le_ids: list of ids of legal entity
    :param exclude: list of parents that should be excluded
    :return: array of parent ids
    """
    return where_in(sess, 'parent', le_ids, 'child', exclude, 'parent')


def get_children(sess: sqlalchemy.orm.Session, parents_ids: set, exclude=None) -> set:
    """
    get all children of parents array
    :param sess: session
    :param parents_ids: list of parents
    :param exclude: list of children that should be excluded
    :return: array of parent ids
    """
    return where_in(sess, 'child', parents_ids, 'parent', exclude, 'child')
