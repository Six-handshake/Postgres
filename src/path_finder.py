from sql_executor import SqlExecutor


def find_paths(executor: SqlExecutor, result_columns: list, le1: str, le2: str, depth=6):
    links = try_get_links(executor, le1, le2, depth)
    if links is None:
        return None
    children, parents, depth = links
    return backtrack(executor, result_columns, le1, le2, children, parents, depth)


def try_get_links(executor: SqlExecutor, le1: str, le2: str, depth: int) -> (list, list, int):
    children = [set() for _ in range(depth)]
    children[0] = {le1}
    parents = [set() for _ in range(depth)]
    current_depth = 0
    while current_depth < depth and le2 not in children[current_depth]:
        parents[current_depth] = get_parents(executor, children[current_depth], parents[current_depth - 1])

        if current_depth - 1 == depth:
            break

        parents_with_children = set()
        parents_with_children.update(parents[current_depth])
        parents_with_children.update(children[current_depth])
        current_children = get_children(executor, parents_with_children, children[current_depth])
        children[current_depth + 1] = current_children
        if current_children is None:
            return None
        current_depth += 1

    if current_depth < depth and le2 in children[current_depth]:
        return children, parents, current_depth
    return None


def backtrack(executor: SqlExecutor, order: list, le1: str, le2: str, children: list, parents: list, depth: int):
    result = get_le_data(executor, order, le2, {'depth': depth})

    children[depth] = {le2}
    depth -= 1
    while depth > 0:
        current_parents = get_parents(executor, children[depth + 1], parents[depth + 1])
        parents[depth] = parents[depth].intersection(current_parents)

        current_children = get_children(executor, parents[depth], children[depth + 1])
        current_parents.update(current_children)
        children[depth] = children[depth].intersection(current_parents)

        children_data = map_data(
            executor.where_in(', '.join(order), 'child', children[depth]),
            order,
            {'depth': depth}
        )

        result.extend(children_data)

        depth -= 1

    result.extend(get_le_data(executor, order, le1, {'depth': 0}))
    return list(reversed(result))


def get_with_filters(executor: SqlExecutor,
                     target_column: str,
                     array_column: str,
                     array: set,
                     exclude_column=None,
                     exclude=None) -> set:
    result = executor.where_in_with_exclude(target_column, array_column, array, exclude_column, exclude)
    if result is None:
        return None
    return set(map(lambda x: str(x[0]), result))


def get_parents(executor: SqlExecutor, le_ids: set, exclude=None) -> set:
    """
    get all parents of legal entity array
    :param executor: SqlExecutor
    :param le_ids: list of ids of legal entity
    :param exclude: list of parents that should be excluded
    :return: array of parent ids
    """
    return get_with_filters(executor, 'parent', 'child', le_ids, 'parent', exclude)


def get_children(executor: SqlExecutor, parents_ids: set, exclude=None) -> set:
    """
    get all children of parents array
    :param executor: SqlExecutor
    :param parents_ids: list of parents
    :param exclude: list of children that should be excluded
    :return: array of parent ids
    """
    return get_with_filters(executor, 'child', 'parent', parents_ids, 'child', exclude)


def map_data(data: list, data_order: list, constants=None) -> list:
    if constants is None:
        constants = dict()
    return list(map(lambda x: {**{data_order[i]: str(x[i]) for i in range(len(data_order))}, **constants}, data))


def get_le_data(executor: SqlExecutor, select: list, le: str, constants=None) -> list:
    return map_data(
        executor.where(', '.join(select), f'child={le}'),
        select,
        constants)
