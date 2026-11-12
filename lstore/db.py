import json
import os
import shutil
import tempfile
from urllib.parse import quote

from lstore import config
from lstore.table import Table
from lstore.bufferpool import BufferPool


class Database():

    def __init__(self):
        self.tables = {}
        self.path = None
        self.bp = None
        self._tmp = None

    def _table_dir(self, name):
        return os.path.join(self.path, quote(name, safe=''))

    def _ensure_pool(self):
        if self.bp is None:
            self._tmp = tempfile.TemporaryDirectory(prefix="lstore_db_")
            self.path = self._tmp.name
            self.bp = BufferPool(config.BUFFERPOOL_SIZE)

    # Not required for milestone1
    def open(self, path, bufferpool_size=None):
        self.path = os.path.abspath(path)
        os.makedirs(self.path, exist_ok=True)
        self.bp = BufferPool(bufferpool_size or config.BUFFERPOOL_SIZE)
        self.tables = {}
        manifest = os.path.join(self.path, "db.json")
        if os.path.exists(manifest):
            with open(manifest) as f:
                names = json.load(f)["tables"]
            for name in names:
                self.tables[name] = Table.load(self._table_dir(name), self.bp)

    def close(self):
        if self.bp is None:
            return
        for t in self.tables.values():
            t.shutdown()
        for t in self.tables.values():
            t.save()
        self.bp.flush_all()
        with open(os.path.join(self.path, "db.json"), "w") as f:
            json.dump({"tables": list(self.tables.keys())}, f)
        self.tables = {}
        self.bp = None
        if self._tmp is not None:
            self._tmp.cleanup()
            self._tmp = None

    def create_table(self, name, num_columns, key_index):
        if name in self.tables:
            raise ValueError("table %s already exists" % name)
        self._ensure_pool()
        table = Table(name, num_columns, key_index, self.bp, self._table_dir(name))
        self.tables[name] = table
        return table

    def drop_table(self, name):
        table = self.tables.pop(name, None)
        if table is None:
            return False
        table.shutdown()
        self.bp.drop_table(name)
        return True

    def get_table(self, name):
        return self.tables.get(name)