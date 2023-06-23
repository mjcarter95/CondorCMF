import logging
import random
import time

import pymysql

from condorcmf import definitions

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

    def connect(self, limit=definitions.MYSQL_MAX_POLL_ATTEMPTS):
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
                sleep_time = random.randint(1, self.poll_delay)
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

    def insert(self, table, columns, values, limit=definitions.MYSQL_MAX_POLL_ATTEMPTS):
        try:
            self.cursor.execute(
                "INSERT INTO {} {} VALUES {}".format(table, columns, values)
            )
            self.connection.commit()
            logging.info(f"Inserted row into {table} table {values}")
        except pymysql.Error as error:
            print("!!!!! INSERT ERROR")
            logging.error(f"Error inserting row into {table} table: {error}")
            if "polling too quickly" in str(error):
                logging.error(
                    f"Waiting {self.poll_delay} seconds before retrying {limit} attempts left..."
                )
                if limit > 1:
                    time.sleep(self.poll_delay)
                    self.insert(table, colums, values, limit - 1)
            return False
        return True

    def select(
        self,
        table,
        columns,
        where_clause,
        orderby=None,
        limit=definitions.MYSQL_MAX_POLL_ATTEMPTS,
    ):
        try:
            query = f"SELECT {columns} FROM {table} WHERE {where_clause}"
            if orderby is not None:
                query += f" ORDER BY {orderby}"
            self.cursor.execute(query)
            result = self.cursor.fetchall()
            logging.info(f"Selected row(s) from {table} table")
        except pymysql.Error as error:
            logging.error(f"Error selecting row(s) from {table} table: {error}")
            print("!!!!! SELECT ERROR")
            # Check for too many connections error
            if "polling too quickly" in str(error):
                logging.error(
                    f"Waiting {self.poll_delay} seconds before retrying {limit} attempts left..."
                )
                if limit > 1:
                    time.sleep(self.poll_delay)
                    self.select(table, columns, where_clause, orderby, limit - 1)
            return False
        return result

    def select_one(
        self,
        table,
        columns,
        where_clause,
        orderby=None,
        limit=definitions.MYSQL_MAX_POLL_ATTEMPTS,
    ):
        try:
            query = f"SELECT {columns} FROM {table} WHERE {where_clause}"
            if orderby is not None:
                query += f" ORDER BY {orderby}"
            self.cursor.execute(query)
            result = self.cursor.fetchone()
            logging.info(f"Selected row(s) from {table} table")
        except pymysql.Error as error:
            print("!!!!! SELECT ONE ERROR")
            logging.error(f"Error selecting row(s) from {table} table: {error}")
            if "polling too quickly" in str(error):
                logging.error(
                    f"Waiting {self.poll_delay} seconds before retrying {limit} attempts left..."
                )
                if limit > 1:
                    time.sleep(self.poll_delay)
                    self.select(table, columns, where_clause, orderby, limit - 1)
            return False
        return result

    def update(
        self, table, set_values, where_clause, limit=definitions.MYSQL_MAX_POLL_ATTEMPTS
    ):
        try:
            self.cursor.execute(
                "UPDATE {} SET {} WHERE {}".format(table, set_values, where_clause)
            )
            self.connection.commit()
            logging.info(f"Updated row(s) in {table} table {set_values}")
        except pymysql.Error as error:
            print("!!!!! UPDATE ERROR")
            logging.error(f"Error updating row(s) in {table} table: {error}")
            if "polling too quickly" in str(error):
                logging.error(
                    f"Waiting {self.poll_delay} seconds before retrying {limit} attempts left..."
                )
                if limit > 1:
                    limit -= 1
                    time.sleep(self.poll_delay)
                    self.update(table, set_values, where_clause, limit)
            return False
        return True

    def delete(self, table, where_clause, limit=definitions.MYSQL_MAX_POLL_ATTEMPTS):
        try:
            self.cursor.execute("DELETE FROM {} WHERE {}".format(table, where_clause))
            self.connection.commit()
            logging.info(f"Deleted row(s) from {table} table")
        except pymysql.Error as error:
            logging.error(f"Error deleting row(s) from {table} table: {error}")
            print("!!!!! DELETE ERROR")
            if "polling too quickly" in str(error):
                logging.error(
                    f"Waiting {self.poll_delay} seconds before retrying {limit} attempts left..."
                )
                if limit > 1:
                    limit -= 1
                    time.sleep(self.poll_delay)
                    self.delete(table, where_clause, limit)
            return False
        return True
