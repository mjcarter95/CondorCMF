import json
import logging
from time import time

from . import utils


class Checkpoint:
    def __init__(self, db, session_id: str, node_id: str, node_type: int):
        self.db = db
        self.session_id = session_id
        self.node_id = node_id
        self.node_type = node_type

    def exists(self, type):
        self.db.connect()
        _exists = self.db.select(
            "checkpoint",
            "id",
            f"session_id = '{self.session_id}' AND `node_id` = '{self.node_id}' AND type = {type}",
        )
        self.db.disconnect()
        if _exists:
            return True
        return False

    def create(self, type, payload):
        logging.info(f"Creating checkpoint for session {self.session_id}")
        self.db.connect()
        _created = self.db.insert(
            "checkpoint",
            "(`session_id`, `node_id`, `node_type`, `created_at`, `type`, `payload`)",
            (
                self.session_id,
                self.node_id,
                self.node_type,
                time(),
                type,
                json.dumps(payload, cls=utils.NpEncoder),
            ),
        )
        self.db.disconnect()
        if _created:
            logging.info(f"Checkpoint created for session {self.session_id}")
            return True
        return False

    def get(self, type):
        self.db.connect()
        _checkpoint = self.db.select_one(
            "checkpoint",
            "payload",
            f"session_id = '{self.session_id}' AND `node_id` = '{self.node_id}' AND type = {type}",
        )
        self.db.disconnect()
        if _checkpoint:
            # Return payload from tuple
            return json.loads(_checkpoint[0])   
        return None

    def update(self, type, payload):
        logging.info(f"Updating checkpoint for session {self.session_id}")
        self.db.connect()
        _updated = self.db.update(
            "checkpoint",
            f"payload = '{json.dumps(payload, cls=utils.NpEncoder)}'",
            f"session_id = '{self.session_id}' AND `node_id` = '{self.node_id}' AND type = {type}",
        )
        self.db.disconnect()
        if _updated:
            logging.info(f"Checkpoint updated for session {self.session_id}")
            return True
        return False

    def delete(self, type):
        logging.info(f"Deleting checkpoint for session {self.session_id}")
        self.db.connect()
        _deleted = self.db.delete(
            "checkpoint",
            f"session_id = '{self.session_id}' AND `node_id` = '{self.node_id}' AND type = {type}",
        )
        self.db.disconnect()
        if _deleted:
            logging.info(f"Checkpoint deleted for session {self.session_id}")
            return True
        return False
