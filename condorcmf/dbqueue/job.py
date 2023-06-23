import json
import logging
import uuid
from time import time

from . import utils

"""
TO DO
    - Ensure that the SQL entries are being created correctly
    - Use insert, update, delete methods from the database class
    - Handle timeout exceptions and max tries
    - Add docstrings
"""


class Job:
    def __init__(
        self,
        db,
        session_id: str,
        to_id: str,
        from_id: str,
        type: int,
        job_id: str = None,
        created_at: float = None,
        deadline: float = None,
        payload=json.dumps({}),
    ):
        self.db = db
        self.session_id = session_id
        self.to_id = to_id
        self.from_id = from_id
        self.type = type
        self.payload = payload
        self.status_code = 0

        self.job_id = str(uuid.uuid4()) if job_id is None else job_id
        self.created_at = time() if created_at is None else created_at
        self.last_updated = time()
        self.deadline = time() + 60 if deadline is None else deadline

    def create(self, status=0, clear_payload=True):
        logging.info(f"Creating job with id: {self.job_id}")
        self.last_updated = time()
        self.db.connect()
        _created = self.db.insert(
            "job_queue",
            "(`session_id`, `job_id`, `to_id`, `from_id`, `type`, `created_at`, `deadline`, `last_updated`, `status_code`, `payload`)",
            (
                self.session_id,
                self.job_id,
                self.to_id,
                self.from_id,
                self.type,
                self.created_at,
                self.deadline,
                self.last_updated,
                status,
                json.dumps(self.payload, cls=utils.NpEncoder),
            ),
        )
        self.db.disconnect()
        if _created:
            if clear_payload:
                self.payload = {}
            logging.info(f"Job created with id {self.job_id}")
            return True
        return False

    def status(self):
        logging.info(f"Getting status of job with id {self.job_id}")
        self.db.connect()
        status = self.db.select_one(
            "job_queue",
            "status_code",
            f"`session_id`='{self.session_id}' AND `job_id`='{self.job_id}'",
        )
        self.db.disconnect()
        logging.info(f"Status of job with id {self.job_id} is {status}")
        self.status_code = status[0]
        return status[0]

    def set_status(self, status: int):
        self.last_updated = time()
        logging.info(f"Setting status of job with id {self.job_id} to {status}")
        self.db.connect()
        self.db.update(
            table="job_queue",
            set_values=f"`status_code` = '{status}'",
            where_clause=f"session_id = '{self.session_id}' AND `job_id` = '{self.job_id}'",
        )
        self.db.disconnect()
        logging.info(f"Status of job with id {self.job_id} set to {status}")
        self.status_code = status

    def set_payload(self, payload: str):
        self.last_updated = time()
        logging.info(f"Setting payload of job with id {self.job_id} to {payload}")
        query = "UPDATE `job` SET payload = %s WHERE job_id = %s"
        self.db.connect()
        self.db.cursor.execute(query, (json.dumps(payload), self.job_id))
        self.db.connection.commit()
        self.db.disconnect()
        logging.info(f"Payload of job with id {self.job_id} set to {payload}")

    def get_payload(self, store_payload=True):
        logging.info(f"Getting payload of job with id {self.job_id}")
        self.db.connect()
        payload = self.db.select_one(
            "job_queue",
            "payload",
            f"`session_id`='{self.session_id}' AND `job_id`='{self.job_id}'",
        )
        self.db.disconnect()
        logging.info(f"Payload of job with id {self.job_id} is {payload}")
        if store_payload:
            self.payload = json.loads(payload[0])
        return json.loads(payload[0])

    def results_available(self, type=None):
        """
        Check if the results of the job are available
        """
        logging.info(f"Checking if results are available for job with id {self.job_id}")
        self.db.connect()
        if type:
            where_clause = f"`session_id`='{self.session_id}' AND `job_id`='{self.job_id}' AND `to_id`='{self.from_id}' AND `from_id`='{self.to_id}' AND `type`='{type}'"
        else:
            f"`session_id`='{self.session_id}' AND `job_id`='{self.job_id}' AND `to_id`='{self.from_id}' AND `from_id`='{self.to_id}'"
        results_available = self.db.select_one(
            "job_queue",
            "status_code",
            where_clause,
        )
        self.db.disconnect()
        logging.info(
            f"Results are available for job with id {self.job_id}: {results_available}"
        )
        return results_available

    def delete(self):
        self.last_updated = time()
        logging.info(f"Deleting job with id {self.job_id}")
        self.db.connect()
        self.db.delete(
            "job_queue",
            f"`session_id`='{self.session_id}' AND `job_id`='{self.job_id}'",
        )
        self.db.disconnect()
        logging.info(f"Job with id {self.job_id} deleted")
