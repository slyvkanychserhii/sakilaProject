
# pip install mysql-connector-python
import mysql.connector


class DB:

    _config = None
    _cnx = None

    def __init__(self, func):
        self.func = func
        if self.__class__._cnx is None:
            try:
                self.__class__._cnx = mysql.connector.connect(**self.__class__._config)
            except Exception as e:
                print(e)

    def __call__(self, *args, **kwargs):
        try:
            cursor = self.__class__._cnx.cursor()
            args = (self.__class__._cnx, cursor,) + args
            result = self.func(*args, **kwargs)
            cursor.close()
            return result
        except Exception as e:
            print(e)

    @classmethod
    def close(cls):
        if cls._cnx is not None:
            try:
                cls._cnx.close()
            except Exception as e:
                print(e)


# # ---------------------------------------------------------------------
# # SIMPLE USAGE
# # ---------------------------------------------------------------------
# class DB1(DB):
#     _config = {'host': 'host',
#                'user': 'user',
#                'password': 'password',
#                'database': 'sakila'}
#
# # ---------------------------------------------------------------------
# @DB1
# def get_data_1(cnx, cursor, query, *params):
#     cursor.execute(query, params)
#     return cursor.fetchall()
#
# query = '''
#     SELECT
#         *
#     FROM film
#     LIMIT %s;
# '''
# for row in get_data_1(query, 5):
#     print(row)
#
# # ---------------------------------------------------------------------
# @DB1
# def get_data_2(cnx, cursor, limit):
#     query = '''
#         SELECT
#             *
#         FROM film
#         LIMIT %s;
#     '''
#     cursor.execute(query, (limit,))
#     return cursor.fetchall()
#
# for row in get_data_2(5):
#     print(row)
#
# # ---------------------------------------------------------------------
# @DB1
# def set_data(cnx, cursor, film):
#     query = '''
#         INSERT INTO film (title, release_year) VALUES (%s, %s)
#     '''
#     cursor.execute(query, (film.title, film.release_year))
#     cnx.commit()
#     return cursor.lastrowid
#
# film = {'title': 'Home Alone', 'release_year': 1990}
# film_id = set_data(film)
# print(film_id)
#
# DB1.close()
