import json
import logging
import uuid
from time import sleep, time

from condorcmf import definitions

try:
    from condorcmf.dbqueue.connector.mysql import MySQLConnector as DBQConnector
except:
    from condorcmf.dbqueue.connector.pymysql import PyMySQLConnector as DBQConnector

from condorcmf.dbqueue.daemon import Daemon
from condorcmf.dbqueue.session import Session

logging.basicConfig(level=logging.INFO)

MYSQL_HOST = ""
MYSQL_USER = ""
MYSQL_PASSWORD = ""
MYSQL_DATABASE = "condorcmf"
MYSQL_POLL_DELAY = 10


def main():
    # Create database connection
    db = DBQConnector(
        MYSQL_HOST,
        MYSQL_USER,
        MYSQL_PASSWORD,
        MYSQL_DATABASE,
        MYSQL_POLL_DELAY,
    )

    # Session parameters
    deadline = 60 * 60 * 7 * 24  # 1 week

    # Instantiate session
    session = Session(db, deadline=deadline)

    # Create session
    session.create(active=True)

    # Coordinator joins pool
    coordinator_daemon = Daemon(db, session.session_id, 0)
    coordinator_daemon.join()

    # Followers join pool
    daemons = []
    for i in range(5):
        daemon = Daemon(db, session.session_id, 1)
        daemon.join()
        daemons.append(daemon)

    print(session.pool_status())

    sleep(5)

    # Followers leave pool
    for daemon in daemons:
        daemon.leave()

    # Coordinator leaves pool
    coordinator_daemon.leave()

    print(session.pool_status())

    sleep(5)

    # Clean up
    session.clear_session()
    session.delete()


if __name__ == "__main__":
    main()
