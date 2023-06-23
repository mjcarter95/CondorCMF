import json
import logging
import uuid
from time import sleep, time

import numpy as np

from condorcmf import definitions

try:
    from condorcmf.dbqueue.connector.mysql import MySQLConnector as DBQConnector
except:
    from condorcmf.dbqueue.connector.pymysql import PyMySQLConnector as DBQConnector

from condorcmf.dbqueue.daemon import Daemon
from condorcmf.dbqueue.job import Job
from condorcmf.dbqueue.session import Session

logging.basicConfig(level=logging.INFO)


def main():
    # Create database connection
    db = DBQConnector(
        definitions.MYSQL_HOST,
        definitions.MYSQL_USER,
        definitions.MYSQL_PASSWORD,
        definitions.MYSQL_DATABASE,
        definitions.MYSQL_POLL_DELAY,
    )

    # Instantiate session
    session_deadline = 60 * 60 * 7 * 24  # 1 week
    session = Session(db, deadline=session_deadline)
    session.create(active=True)

    # Daemons join sesion pool
    coordinator_daemon = Daemon(db, session.session_id, 0)
    coordinator_daemon.join()

    n_followers = 10
    follower_daemons = []
    for i in range(n_followers):
        daemon = Daemon(db, session.session_id, 1)
        daemon.join()
        follower_daemons.append(daemon)

    print(f"Pool status: {session.pool_status()}")

    # Create a job for each follower
    deadline = 60 * 60  # 1 hour
    global_sum = 0
    data = []
    follower_jobs = []
    for i in range(n_followers):
        data.append(np.random.randint(1, 100, size=10))
        payload = {"data": data[i]}
        global_sum += np.sum(data[i])
        job = Job(
            db,
            session.session_id,
            follower_daemons[i].node_id,
            coordinator_daemon.node_id,
            0,
            deadline=deadline,
            payload=payload,
        )
        job.create()
        follower_jobs.append(job)

    # Print status of jobs
    for follower_job in follower_jobs:
        print(follower_job.status())

    # Each follower fetches and completes its job
    # the results are then sent to the coordinator
    for i in range(n_followers):
        # Fetch latest job from queue
        follwer_job = follower_daemons[i].fetch_job()

        # Set job status to 1 (in progress)
        follwer_job.set_status(1)

        # Complete job
        follower_sum = np.sum(follwer_job.payload["data"])
        print(f"Worker sum: {follower_sum}")

        # Set job status to 2 (completed)
        follwer_job.set_status(2)

        # Send result to coordinator
        coordinator_job = Job(
            db,
            session.session_id,
            follower_job.from_id,
            follower_job.to_id,
            1,
            deadline=deadline,
            payload={"data": follower_sum},
        )
        coordinator_job.create()

    # Coordinator fetches and completes its jobs
    coordinator_sum = 0
    coordinator_jobs = coordinator_daemon.fetch_all_jobs()
    for coordinator_job in coordinator_jobs:
        # Set job status to 1 (in progress)
        coordinator_job.set_status(1)

        # Complete job
        coordinator_sum += coordinator_job.payload["data"]

        # Set job status to 2 (completed)
        coordinator_job.set_status(2)

    print(f"Coordinator sum: {coordinator_sum}, Global sum: {global_sum}")

    # Clean up
    session.clear_session()
    session.delete()

    # Check if global sum is equal to sum of all follower sums
    assert (
        global_sum == coordinator_sum
    ), "Global sum is not equal to sum of all follower sums"


if __name__ == "__main__":
    main()
