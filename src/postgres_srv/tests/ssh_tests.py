import sqlalchemy.orm
from postgres_srv.config import *
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

        result = find_paths(session, le1, le2)

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


def execute(sess: sqlalchemy.orm.Session, query: sqlalchemy.Text):
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


def where_in(conn,
             target: str,
             array: set,
             array_column: str,
             exclude=None,
             exclude_column=None) -> list:
    if array is None or len(array) == 0:
        return None

    sql_array = sql_subquery_transform(array)
    sql_exclude = sql_subquery_transform(exclude) if exclude is not None and len(exclude) != 0 else None

    sql = text(f'SELECT {target} FROM "{TABLE_NAME}" WHERE ({array_column} IN {sql_array}' +
               (f' AND {exclude_column} NOT IN {sql_exclude})' if sql_exclude is not None else ')'))

    return execute(conn, sql)


def get_with_filters(conn,
                     target_column: str,
                     array: set,
                     array_column: str,
                     exclude=None,
                     exclude_column=None) -> set:
    result = where_in(conn, target_column, array, array_column, exclude, exclude_column)
    if result is None:
        return None
    return set(map(lambda x: str(x[0]), result))


def get_parents(conn, le_ids: set, exclude=None) -> set:
    """
    get all parents of legal entity array
    :param conn: connection
    :param le_ids: list of ids of legal entity
    :param exclude: list of parents that should be excluded
    :return: array of parent ids
    """
    return get_with_filters(conn, 'parent', le_ids, 'child', exclude, 'parent')


def get_children(conn, parents_ids: set, exclude=None) -> set:
    """
    get all children of parents array
    :param conn: connection
    :param parents_ids: list of parents
    :param exclude: list of children that should be excluded
    :return: array of parent ids
    """
    return get_with_filters(conn, 'child', parents_ids, 'parent', exclude, 'child')


def map_data(data: list, data_order: list, constants=None) -> list:
    if constants is None:
        constants = dict()
    return list(map(lambda x: {**{data_order[i]: x[i] for i in range(len(data_order))}, **constants}, data))


def get_le_data(conn, le: str, constants=None) -> list:
    return map_data(
        execute(conn, text(f'SELECT child, parent, kind FROM "{TABLE_NAME}" WHERE child={le}')),
        ['child', 'parent', 'kind'],
        constants)


def find_paths(conn, le1: str, le2: str, depth=6):
    links = try_get_links(conn, le1, le2, depth)
    if links is None:
        return None
    children, parents, depth = links
    return backtrack(conn, le1, le2, children, parents, depth)


def try_get_links(conn, le1: str, le2: str, depth: int) -> (list, list, int):
    children = [set() for i in range(depth)]
    children[0] = {le1}
    parents = [set() for i in range(depth)]
    current_depth = 0
    while current_depth < depth and le2 not in children[current_depth]:
        parents[current_depth] = get_parents(conn, children[current_depth], parents[current_depth - 1])

        if current_depth - 1 == depth:
            break

        children[current_depth + 1] = get_children(conn, parents[current_depth], children[current_depth])
        current_depth += 1

    if current_depth < depth and le2 in children[current_depth]:
        return children, parents, current_depth
    return None


def backtrack(conn, le1: str, le2: str, children: list, parents: list, depth: int):
    result = get_le_data(conn, le2, {'depth': depth})

    children[depth] = {le2}
    depth -= 1
    while depth > 0:

        parents[depth] = parents[depth].intersection(get_parents(conn, children[depth + 1], parents[depth + 1]))

        order = ['child', 'parent', 'kind']
        children_data = map_data(
            where_in(conn, ', '.join(order), parents[depth], 'parent', children[depth + 1], 'child'),
            order,
            {'depth': depth})

        children[depth] = children[depth].intersection(set(map(lambda x: x['child'], children_data)))

        result.extend(children_data)

        depth -= 1

    result.extend(get_le_data(conn, le1, {'depth': 0}))
    return list(reversed(result))
