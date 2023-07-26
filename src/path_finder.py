from sql_executor import SqlExecutor


def find_paths(executor: SqlExecutor, result_columns: list, le1: str, le2: str, depth=6):
    links = try_get_links(executor, le1, le2, depth + 1)
    if links is None:
        return None
    children, parents, depth = links
    result = backtrack(executor, result_columns, le1, le2, children, parents, depth)
    return link_objects(result)


def find_all_links(executor: SqlExecutor, result_columns: list, le: str, depth=6):
    children, parents, current_depth = try_get_links(executor, le, None, depth + 1)
    return link_objects(transform_children(executor, children, parents, result_columns))


def transform_children(executor: SqlExecutor, children: list, parents: list, order: list):
    result = []
    for i in range(len(children)):
        if len(children[i]) == 0:
            break
        children[i] = set(sorted(children[i])[:100])
        result.extend(map_data(
            executor.where_in(order, 'child', children[i]).where_raw(f'(kind=2)').order_by('share').execute(),
            order,
            {'depth': i}
        ))
    return result


def try_get_links(executor: SqlExecutor, le1: str, le2: str, depth: int) -> (list, list, int):
    children = [set() for _ in range(depth)]
    children[0] = {le1}
    parents = [set() for _ in range(depth)]
    current_depth = 0
    exclude_children = {le1}
    exclude_parents = set()
    while current_depth < depth and (le2 is None or le2 not in children[current_depth]):
        if len(children[current_depth]) == 0:
            break
        parents[current_depth] = get_parents(executor, children[current_depth], parents[current_depth - 1])
        exclude_parents.update(parents[current_depth])

        if current_depth + 1 == depth:
            break

        parents_with_children = merge_sets(parents[current_depth], children[current_depth])
        current_children = get_children(executor, parents_with_children, exclude_children)
        exclude_children.update(current_children)
        children[current_depth + 1] = current_children
        if current_children is None:
            return None
        current_depth += 1

    if le2 is None or (current_depth < depth and le2 in children[current_depth]):
        return children, parents, current_depth
    return None


def backtrack(executor: SqlExecutor, order: list, le1: str, le2: str, children: list, parents: list, depth: int):
    result = get_le_data(executor, order, le2, {'depth': depth})

    children[depth] = {le2}
    depth -= 1
    filter_cond = lambda p: f'(kind=2 OR parent IN {SqlExecutor.array_to_sql_array(p)})'
    while depth > 0:
        current_parents = get_parents(executor, children[depth + 1], parents[depth + 1])
        parents[depth] = parents[depth].intersection(current_parents)

        current_children = get_children(executor, parents[depth], children[depth + 1])
        current_parents.update(current_children)
        children[depth] = children[depth].intersection(current_parents)

        common_parents = merge_sets(parents[depth], parents[depth - 1])
        children_data = map_data(
            executor
                .where_in(order, 'child', children[depth])
                .where_raw(filter_cond(common_parents))
                .order_by('share')
                .execute(),
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
    result = executor.where_in_with_exclude([target_column], array_column, array, exclude_column, exclude).execute()
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
        executor.select(select).where('child', '=', le).order_by('share').execute(),
        select,
        constants)


def merge_sets(set1: set, set2: set) -> set:
    if set1 is None or set2 is None:
        return None

    set3 = set()
    set3.update(set1)
    set3.update(set2)
    return set3


def link_objects(objects: list):
    if objects is None or len(objects) == 0:
        return objects

    p_name = 'links'
    depth_id = {0: 0}

    current_depth = 0
    for i, e in enumerate(objects):
        if e['depth'] != current_depth:
            current_depth = e['depth']
            depth_id[current_depth] = i

        e[p_name] = []
        if current_depth == 0:
            continue

        for j in range(depth_id[current_depth - 1], depth_id[current_depth]):
            possible_ancestor = objects[j]
            if e['parent'] not in [possible_ancestor['child'], possible_ancestor['parent']]:
                continue
            e[p_name].append({
                'child_id': possible_ancestor['child'],
                'object_id': j,
                'type': 'child' if possible_ancestor['child'] == e['parent'] else 'parent'
            })

    return objects

