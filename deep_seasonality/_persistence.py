import pymssql
from _config import Config, Token


class Persistence:

    def __init__(self, sqls_path, key_file):
        self._sqls_path = sqls_path

        try:
            token = Token(key_file)

            server = token.get_key('server')
            port = token.get_key('port')
            database = token.get_key('database')
            user = token.get_key('uid')
            password = token.get_key('pwd')

            if port != '':
                self._connection = pymssql.connect(server=server, port=port, database=database, user=user,
                                                    password=password)
            else:
                self._connection = pymssql.connect(server=server, database=database, user=user, password=password)

        except pymssql.Error as fail:
            raise Exception('exception connection, fail : {}'.format(fail))

    def sql_query(self, file, table_columns, params={}):
        try:
            sql = self.get_file_sql(file)

            for key, value in params.items():
                sql = sql.replace(key, value)

            cursor = self._connection.cursor()
            cursor.execute(sql)
            row = cursor.fetchone()

            response = []
            while row:
                row_json = {}
                index_column = 0
                for column in table_columns:
                    row_json.update({column: row[index_column]})
                    index_column += 1
                response.append(row_json)
                row = cursor.fetchone()

            return response
        except Exception as fail:
            raise Exception('exception get sql {}, fail: {}'.format(file, fail))

    def sql_update(self, file, list_update):
        try:
            sql = self.get_file_sql(file)

            cursor = self._connection.cursor()
            cursor.executemany(sql, list_update)

            self._connection.commit()
        except Exception as fail:
            self._connection.rollback()
            raise Exception('exception update sql {}, fail: {}'.format(file, fail), True)

    def sql_execute(self, file):
        try:
            sql = self.get_file_sql(file)

            cursor = self._connection.cursor()
            cursor.execute(sql)

            self._connection.commit()
        except Exception as fail:
            self._connection.rollback()
            raise Exception('exception update sql {}, fail: {}'.format(file, fail), True)

    def get_file_sql(self, file_name):
        with open('{}\\{}.sql'.format(self._sqls_path, file_name)) as file:
            sql = file.read()
        return sql
