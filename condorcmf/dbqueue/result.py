import json
import logging
import uuid
from time import time


class Result:
    def __init__(
        self,
        db,
        session_id: str,
        node_id: str,
        results_id: str = None,
        attributes=json.dumps({}),
        payload=json.dumps({}),
    ):
        self.db = db
        self.session_id = session_id
        self.node_id = node_id
        self.role = role
        self.results_id = str(uuid.uuid4()) if results_id is None else results_id
        self.attributes = attributes
        self.payload = payload

    def insert(self):
        res = self.db.insert(
            "results",
            "(`id`, `session_id`, `node_id`, `role`, `attributes`, `payload`, `created_at`)",
            (self.session_id, self.node_id, self.role, self.attributes, self.payload, time()),
        )
        return res

    def fetch(self):
        results = self.db.select(
            "results",
            "*",
            "WHERE `results_id` = %s",
        )

    def delete(self):
        self.db.delete("results", "results_id", self.results_id)