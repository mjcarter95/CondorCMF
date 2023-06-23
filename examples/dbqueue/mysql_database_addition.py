import json
import logging
import time
import uuid

import numpy as np

from condorcmf import definitions

try:
    from condorcmf.dbqueue.connector.mysql import MySQLConnector as DBQConnector
except:
    from condorcmf.dbqueue.connector.pymysql import PyMySQLConnector as DBQConnector

logging.basicConfig(level=logging.INFO)

MYSQL_HOST = ""
MYSQL_USER = ""
MYSQL_PASSWORD = ""
MYSQL_DATABASE = "condorcmf"
MYSQL_POLL_DELAY = 10


def main():
    db = DBQConnector(
        MYSQL_HOST,
        MYSQL_USER,
        MYSQL_PASSWORD,
        MYSQL_DATABASE,
        MYSQL_POLL_DELAY,
    )

    n_jobs = 100
    session_id = str(uuid.uuid4())

    global_sum = 0
    data = []

    # Create jobs
    for i in range(n_jobs):
        data.append(np.random.randint(1, 100, size=10))
        global_sum += np.sum(data[i])
        data[i] = json.dumps(data[i].tolist())

    # Insert jobs
    query = (
        "INSERT INTO `job_queue` (session_id, job_id, to_id, from_id, type, created_at, deadline, last_updated, status_code, payload)"
        "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    )

    db.connect()
    for i in range(n_jobs):
        job_id = str(uuid.uuid4())
        created_at = time.time()
        db.cursor.execute(
            query,
            (
                session_id,
                job_id,
                i,
                0,
                0,
                created_at,
                created_at,
                created_at,
                0,
                data[i],
            ),
        )
    db.connection.commit()
    db.disconnect()

    worker_sum = 0

    # Process jobs
    select_query = "SELECT * FROM `job_queue` WHERE `session_id` = %s AND `to_id` = %s AND `status_code` = %s"
    update_query = "UPDATE `job_queue` SET `status_code` = %s WHERE `session_id` = %s AND `to_id` = %s AND `status_code` = %s"

    db.connect()
    for i in range(n_jobs):
        db.cursor.execute(select_query, (session_id, i, 0))
        result = db.cursor.fetchall()

        db.cursor.execute(update_query, (1, session_id, i, 0))
        db.connection.commit()

        values = np.array(json.loads(result[0][-1]))
        worker_sum += np.sum(values)

        db.cursor.execute(update_query, (2, session_id, i, 1))
        db.connection.commit()

    db.disconnect()

    if global_sum == worker_sum:
        print("Success!")

    print(f"Global sum: {global_sum} | Worker sum: {worker_sum}")

    # Tidy up jobs
    db.connect()
    db.cursor.execute("DELETE FROM `job_queue` WHERE `session_id` = %s", (session_id,))
    db.connection.commit()
    db.disconnect()


if __name__ == "__main__":
    main()
