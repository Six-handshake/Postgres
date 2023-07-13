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

    def select_from(self, columns: str, table: str, ending=''):
        return self.execute(self.connection, f'SELECT {columns} FROM "{table}"' + f' {ending}' if len(ending) > 0 else '')

    def select(self, columns: str, ending=''):
        return self.select_from(columns, self.table, ending)

    def where(self, select: str, where_clause: str):
        return self.select(select, f'WHERE {where_clause}')

    def where_in(self, select_columns: str, target_column: str, target_array):
        sql_array = SqlExecutor.array_to_sql_array(target_array)

        if sql_array is None:
            return None

        return self.where(select_columns, f'{target_column} IN {sql_array}')

    def where_in_with_exclude(self,
                              select: str,
                              target_column: str,
                              target_array,
                              exclude_column=None,
                              exclude_array=None):
        target_sql = SqlExecutor.array_to_sql_array(target_array)

        if target_sql is None:
            return None

        exclude_sql = SqlExecutor.array_to_sql_array(exclude_array)

        clause = f'({target_column} in {target_sql}' + \
                 (f' AND {exclude_column} NOT IN {exclude_sql})'
                  if exclude_column is not None and exclude_sql is not None else ')')
        return self.where(select, clause)
