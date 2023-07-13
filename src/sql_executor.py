class SqlExecutor:
    def __init__(self, connection, execute, table=''):
        self.connection = connection
        self.execute = execute
        self.table = table

    @staticmethod
    def array_to_sql_array(array):
        """
        transform array to sql subquery
        :param array: array that should be transformed
        :return: subquery string
        """
        if array is None:
            return None

        if array is str:
            return array

        if len(array) == 0:
            return None

        return '(' + ', '.join(array) + ')'

    def raw_sql(self, sql: str):
        return self.execute(self.connection, sql)

    def select_from(self, columns: list, table: str):
        return SqlBuilder(self).select(*columns).from_table(table)

    def select(self, columns: list):
        return SqlBuilder(self).select(*columns).from_table(self.table)

    def where_in(self, select_columns: list, target_column: str, target_array):
        sql_array = SqlExecutor.array_to_sql_array(target_array)

        if sql_array is None:
            return None

        return self.select(select_columns).where(target_column, ' IN ', sql_array)

    def where_in_with_cond(self,
                           select_columns: list,
                           target_column: str,
                           target_array,
                           cond: str):
        target_sql = SqlExecutor.array_to_sql_array(target_array)

        if target_sql is None:
            return None

        return self.select(select_columns).where(target_column, ' IN ', target_sql).where_raw(cond)

    def where_in_with_one_cond(self,
                               select_columns: list,
                               target_column: str,
                               target_array,
                               cond_column: str,
                               cond_value: str,
                               cond_op='='):
        target_sql = SqlExecutor.array_to_sql_array(target_array)

        if target_sql is None:
            return None

        return self.select(select_columns).where(target_column, ' IN ', target_sql).where(cond_column, cond_op, cond_value)

    def where_in_with_exclude(self,
                              select: list,
                              target_column: str,
                              target_array,
                              exclude_column=None,
                              exclude_array=None):
        target_sql = SqlExecutor.array_to_sql_array(target_array)

        if target_sql is None:
            return None

        exclude_sql = SqlExecutor.array_to_sql_array(exclude_array)

        if exclude_column is None or exclude_sql is None:
            return self.where_in(select, target_column, target_array)

        return self.select(select).where(target_column, ' IN ', target_sql).where(exclude_column, ' NOT IN ', exclude_sql)


class SqlBuilder:
    def __init__(self, executor: SqlExecutor):
        self._executor = executor
        self._select = []
        self._table = None
        self._where = []
        self._order_by = None
        self._limit = None

    def select(self, *args):
        self._select.extend(args)
        return self

    def from_table(self, table: str):
        self._table = table
        return self

    def where(self, first: str, op: str, second: str):
        self._where.append([[first, op, second], ' AND '])
        return self

    def where_raw(self, clause: str):
        self._where.append([clause, ' AND '])
        return self

    def or_where(self, first: str, op: str, second: str):
        self._where.append([[first, op, second], ' OR '])
        return self

    def or_where_raw(self, clause: str):
        self._where.append([clause, ' OR '])
        return self

    def order_by(self, column: str):
        self._order_by = column
        return self

    def limit(self, count: int):
        self._limit = count
        return self

    def _get_str_where(self) -> str:
        if len(self._where) == 0:
            return ''

        result = ''.join(self._where[0][0])
        for i in range(1, len(self._where)):
            result = result + self._where[i][1] + ''.join(self._where[i][0])
        return result

    def execute(self):
        if len(self._select) == 0 or self._table is None:
            return None

        sql = f'SELECT {", ".join(self._select)} FROM "{self._table}"' + \
              (f' WHERE ({self._get_str_where()})' if len(self._where) > 0 else '') + \
              (f' ORDER BY {self._order_by}' if self._order_by is not None else '') + \
              (f' LIMIT {self._limit}' if self._limit is not None else '')

        return self._executor.raw_sql(sql)

