import logging
import random
import time

import pymysql

"""
TO DO
    - Add docstrings
"""


class PyMySQLConnector:
    def __init__(self, host, user, password, database, poll_delay=5, max_retries=5):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.poll_delay = poll_delay
        self.max_retries = max_retries
        self.connection = None
        self.cursor = None

    def connect(self, limit=10):
        try:
            self.connection = pymysql.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database,
            )
            self.cursor = self.connection.cursor()
            logging.info("Connected to MySQL database")
        except pymysql.Error as error:
            print("!!!!! CONNECT ERROR")
            logging.error("Failed to connect to MySQL database: {}".format(error))
            error_code = error.args[0]
            if error_code == 1040:
                print("too many connections")
                sleep_time = random.random()
                logging.error(
                    f"Waiting {sleep_time} seconds before retrying {limit} attempts left..."
                )
                if limit > 1:
                    self.connection = None
                    time.sleep(sleep_time)
                    self.connect(limit - 1)

    def disconnect(self):
        if self.connection is not None:
            self.cursor.close()
            self.connection.close()
            self.connection = None
            logging.info("Disconnected from MySQL database")

    def insert(self, table, columns, values):
        query = f"INSERT INTO {table} {columns} VALUES {values}"
        res = self._execute_query(query)
        return res

    def select(
        self,
        table,
        columns,
        where_clause,
        orderby=None,
    ):
        query = f"SELECT {columns} FROM {table} WHERE {where_clause}"
        if orderby is not None:
            query += f" ORDER BY {orderby}"
        res = self._execute_query(query, select=True)
        return res

    def select_one(
        self,
        table,
        columns,
        where_clause,
        orderby=None,
    ):
        query = f"SELECT {columns} FROM {table} WHERE {where_clause}"
        if orderby is not None:
            query += f" ORDER BY {orderby}"
        res = self._execute_query(query, select_one=True)
        return res
        

    def update(
        self, table, set_values, where_clause
    ):
        query = f"UPDATE {table} SET {set_values} WHERE {where_clause}"
        res = self._execute_query(query)
        return res

    def delete(self, table, where_clause):
        query = f"DELETE FROM {table} WHERE {where_clause}"
        res = self._execute_query(query)
        return res

    def _execute_query(self, query, select=False, select_one=False, limit=10):
        try:
            if select:
                self.connect()
                self.cursor.execute(query)
                res = self.cursor.fetchall()
                self.disconnect()
                if not res:
                    return None
                return res
            elif select_one:
                self.connect()
                self.cursor.execute(query)
                res = self.cursor.fetchone()
                self.disconnect()
                if not res:
                    return None
                return res
            else:
                self.connect()
                self.cursor.execute(query)
                self.connection.commit()
                self.disconnect()
                return True
        except pymysql.Error as error:
            error_code = error.args[0]
            if error_code == 1040:
                print("too many connections")
                sleep_time = random.random()
                logging.error(
                    f"Waiting {sleep_time} seconds before retrying {limit} attempts left..."
                )
                if limit > 1:
                    self.connection = None
                    time.sleep(sleep_time)
                    self.connect(limit - 1)
            if "polling too quickly" in str(error):
                logging.error(
                    f"Waiting {self.poll_delay} seconds before retrying {limit} attempts left..."
                )
                if limit > 1:
                    time.sleep(self.poll_delay)
                    self.insert(table, colums, values, limit - 1)
            return False