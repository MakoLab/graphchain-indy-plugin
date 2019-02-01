import os
from threading import Timer

import rocksdb
from stp_core.common.log import getlogger

logger = getlogger()


INTERVAL = 60
PRIORITY = 1
UTF_8 = "utf-8"


class GraphStoreSynchronizer:
    def __init__(self, data_dir: str, name: str):
        logger.info("Initializing graph store synchronizer...")
        self._data_dir = data_dir
        self._name = name
        self._store_path = os.path.join(self._data_dir, self._name)
        self.db = rocksdb.DB(self._store_path,
                             rocksdb.Options(
                                 create_if_missing=True))

        self._scheduled_job = None

    def start(self, scheduled_job):
        logger.debug("Starting scheduler for graph store synchronizer...")

        self._scheduled_job = scheduled_job
        t = Timer(INTERVAL, self._run_job)
        t.daemon = True
        t.start()

    def add(self, key: bytes, value: bytes):
        logger.debug("Adding a new pair to synchronizer: {} => {}".format(key, value))
        self.db.put(key, value)

    def exists(self, key: bytes) -> bool:
        return self.db.get(key) is not None

    def remove(self, key: bytes):
        try:
            self.db.delete(key)
        except rocksdb.errors.NotFound as ex:
            logger.warning("Attempting to remove unexisting entry with key '{}'. Details: {}"
                           .format(key.decode(UTF_8), ex))

    def list_all(self):
        it = self.db.iteritems()
        it.seek_to_first()
        return it

    def _run_job(self):
        logger.debug("Running scheduled job...")
        t = Timer(INTERVAL, self._run_job)
        t.daemon = True
        t.start()
        self._scheduled_job()
