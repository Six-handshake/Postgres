import psycopg2 as pg
from ..config import *


def get_links(le1: str, le2: str):
    conn = connect_to_db()

    data = find_paths(conn, le1, le2)

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
    print(query)
    print(conn)
    if conn is None:
        return None

    try:
        print('Trying execute...')
        with conn.cursor as cursor:
            print('cursor created')
            cursor.execute(query)
            print('cursor executed')
            res = cursor.fetchall()
            print('cursor fetched')
            print(res)
            print('executed!')
            return res
    except Exception as e:
        print('EXCEPTION!!!!')
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

    sql = f'SELECT {target} FROM "{TABLE_NAME}" WHERE ({array_column} IN {sql_array}' + \
          (f' AND {exclude_column} NOT IN {sql_exclude})' if sql_exclude is not None else ')')

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
    return list(map(lambda x: {**{data_order[i]: str(x[i]) for i in range(len(data_order))}, **constants}, data))


def get_le_data(conn, le: str, constants=None) -> list:
    return map_data(
        execute(conn, f'SELECT child, parent, kind FROM "{TABLE_NAME}" WHERE child={le}'),
        ['child', 'parent', 'kind'],
        constants)


def find_paths(conn, le1: str, le2: str, depth=6):
    links = try_get_links(conn, le1, le2, depth)
    if links is None:
        return None
    children, parents, depth = links
    return backtrack(conn, le1, le2, children, parents, depth)


def try_get_links(conn, le1: str, le2: str, depth: int) -> (list, list, int):
    children = [set() for _ in range(depth)]
    children[0] = {le1}
    parents = [set() for _ in range(depth)]
    current_depth = 0
    while current_depth < depth and le2 not in children[current_depth]:
        parents[current_depth] = get_parents(conn, children[current_depth], parents[current_depth - 1])

        if current_depth - 1 == depth:
            break

        current_children = get_children(conn, parents[current_depth], children[current_depth])
        children[current_depth + 1] = current_children
        if current_children is None:
            return None
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

        children[depth] = children[depth].intersection(get_children(conn, parents[depth], children[depth + 1]))

        children_data = map_data(
            where_in(conn, ', '.join(order), children[depth], 'child'),
            order,
            {'depth': depth}
        )

        result.extend(children_data)

        depth -= 1

    result.extend(get_le_data(conn, le1, {'depth': 0}))
    return list(reversed(result))
