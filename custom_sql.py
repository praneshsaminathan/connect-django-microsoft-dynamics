from django.db import connection


class ExecuteRawSQL:
    @staticmethod
    def fetchone_dict(query):
        with connection.cursor() as cursor:
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            row = cursor.fetchone()
        if row:
            return [dict(zip(columns, row))]

        return None

    @staticmethod
    def fetch_list(query):
        with connection.cursor() as cursor:
            cursor.execute(query)
            columns = [col[0] for col in cursor.description]
            rows = cursor.fetchall()

        if rows:
            return [dict(zip(columns, row)) for row in rows]

        return None
