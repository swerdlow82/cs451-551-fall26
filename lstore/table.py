import json
import os
import queue
import tempfile
import threading
import traceback
from array import array
from time import time
from urllib.parse import quote

from lstore import config
from lstore.config import *
from lstore.index import Index
from lstore.bufferpool import BufferPool


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    def __getitem__(self, i):          
        return self.columns[i]

    def __repr__(self):
        return "Record(rid=%s, key=%s, columns=%s)" % (self.rid, self.key, self.columns)


class PageRange:

    def __init__(self):
        self.base_records = 0
        self.tail_records = 0
        self.merge_pos = 0
        self.tps = TPS_INITIAL
        self.last_tid = 0
        self.merge_pending = False


class Table:

    def __init__(self, name, num_columns, key, bufferpool=None, directory=None):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.num_phys_cols = NUM_META_COLUMNS + num_columns

        self._tmp = None
        if bufferpool is None:                        
            self._tmp = tempfile.TemporaryDirectory(prefix="lstore_")
            bufferpool = BufferPool(config.BUFFERPOOL_SIZE)
            directory = os.path.join(self._tmp.name, quote(name, safe=''))
        self.bp = bufferpool
        self.directory = directory
        self.bp.register_table(name, directory, self.num_phys_cols)

        self.page_directory = {}
        self.page_ranges = []
        self.next_base_rid = 1
        self.next_tail_rid = TAIL_RID_START
        self.col_updated = [False] * num_columns     

        self.lock = threading.RLock()                
        self._merge_q = queue.Queue()
        self._merge_thread = None
        self.merges_done = 0

        self.index = Index(self)

    def _read_cols(self, rid, cols):
        ridx, is_tail, pidx, slot = self.page_directory[rid]
        key = (ridx, is_tail, pidx)
        ps = self.bp.pin(self.name, key)
        try:
            pages = ps.pages
            return [pages[c].read(slot) for c in cols]
        finally:
            self.bp.unpin(self.name, key)

    def _write_cols(self, rid, items):
        ridx, is_tail, pidx, slot = self.page_directory[rid]
        key = (ridx, is_tail, pidx)
        ps = self.bp.pin(self.name, key)
        try:
            for c, v in items:
                ps.pages[c].overwrite(slot, v)
        finally:
            self.bp.unpin(self.name, key, True)

    def _append(self, ridx, is_tail, n, row):
        pidx, slot = divmod(n, RECORD_PER_PAGE)
        key = (ridx, is_tail, pidx)
        if slot == 0:
            ps = self.bp.new_page(self.name, key)
        else:
            ps = self.bp.pin(self.name, key)
        try:
            got = ps.append_row(row)
            assert got == slot
        finally:
            self.bp.unpin(self.name, key, True)
        return pidx, slot

    def _append_tail(self, ridx, prev_ptr, schema, base_rid, values):
        pr = self.page_ranges[ridx]
        tid = self.next_tail_rid
        self.next_tail_rid -= 1
        row = [prev_ptr, tid, int(time()), schema, base_rid] + list(values)
        pidx, slot = self._append(ridx, True, pr.tail_records, row)
        pr.tail_records += 1
        pr.last_tid = tid
        self.page_directory[tid] = (ridx, True, pidx, slot)
        return tid

    def insert_record(self, columns):
        with self.lock:
            if not self.page_ranges or \
                    self.page_ranges[-1].base_records >= BASE_PAGES_PER_RANGE * RECORD_PER_PAGE:
                self.page_ranges.append(PageRange())
            ridx = len(self.page_ranges) - 1
            pr = self.page_ranges[ridx]
            rid = self.next_base_rid
            self.next_base_rid += 1
            row = [NULL_POINTER, rid, int(time()), 0, rid] + list(columns)
            pidx, slot = self._append(ridx, False, pr.base_records, row)
            pr.base_records += 1
            self.page_directory[rid] = (ridx, False, pidx, slot)
            for c in range(self.num_columns):
                self.index.insert_entry(c, columns[c], rid)
            return rid

    def _src(self, base_rid, relative_version=0):
        ridx = self.page_directory[base_rid][0]
        ind = self._read_cols(base_rid, (INDIRECTION_COLUMN,))[0]
        if ind == NULL_POINTER:
            return base_rid
        if relative_version >= 0:
            return base_rid if ind >= self.page_ranges[ridx].tps else ind
        cur = ind
        for _ in range(-relative_version):
            prev = self._read_cols(cur, (INDIRECTION_COLUMN,))[0]
            if prev == base_rid:          
                break
            cur = prev
        return cur

    def read_record(self, base_rid, projected, relative_version=0):
        with self.lock:
            src = self._src(base_rid, relative_version)
            cols = [NUM_META_COLUMNS + c for c in range(self.num_columns) if projected[c]]
            vals = iter(self._read_cols(src, cols))
            return [next(vals) if projected[c] else None for c in range(self.num_columns)]

    def read_value(self, base_rid, column, relative_version=0):
        with self.lock:
            src = self._src(base_rid, relative_version)
            return self._read_cols(src, (NUM_META_COLUMNS + column,))[0]

    def exists(self, rid):
        return rid in self.page_directory

    def update_record(self, base_rid, columns):
        with self.lock:
            ridx = self.page_directory[base_rid][0]
            pr = self.page_ranges[ridx]
            ind, schema = self._read_cols(base_rid, (INDIRECTION_COLUMN, SCHEMA_ENCODING_COLUMN))
            data_cols = [NUM_META_COLUMNS + c for c in range(self.num_columns)]
            if ind == NULL_POINTER:
                cur_vals = self._read_cols(base_rid, data_cols)
                ind = self._append_tail(ridx, base_rid, 0, base_rid, cur_vals)   
            else:
                cur_vals = self._read_cols(ind, data_cols)
            new_vals = list(cur_vals)
            for c, v in enumerate(columns):
                if v is not None:
                    new_vals[c] = v
                    self.col_updated[c] = True
                    schema |= (1 << c)
            tid = self._append_tail(ridx, ind, schema, base_rid, new_vals)
            self._write_cols(base_rid, [(INDIRECTION_COLUMN, tid), (SCHEMA_ENCODING_COLUMN, schema)])

            for c in range(self.num_columns):
                if new_vals[c] != cur_vals[c]:
                    self.index.remove_entry(c, cur_vals[c], base_rid)
                    self.index.insert_entry(c, new_vals[c], base_rid)

            if (pr.tail_records - pr.merge_pos >= config.MERGE_THRESHOLD) and not pr.merge_pending:
                pr.merge_pending = True
                self._enqueue_merge(ridx)

    def delete_record(self, base_rid):
        with self.lock:
            vals = self.read_record(base_rid, [1] * self.num_columns)
            for c in range(self.num_columns):
                self.index.remove_entry(c, vals[c], base_rid)
            cur = self._read_cols(base_rid, (INDIRECTION_COLUMN,))[0]
            while cur != NULL_POINTER and cur != base_rid:
                prev = self._read_cols(cur, (INDIRECTION_COLUMN,))[0]
                self._write_cols(cur, [(RID_COLUMN, INVALID_POINTER)])
                del self.page_directory[cur]
                cur = prev
            self._write_cols(base_rid, [(RID_COLUMN, INVALID_POINTER)])
            del self.page_directory[base_rid]

    def scan_latest(self, column):
        with self.lock:
            rids = [rid for rid, loc in self.page_directory.items() if not loc[1]]
            return [(rid, self.read_value(rid, column)) for rid in rids]


    def _enqueue_merge(self, ridx):
        if self._merge_thread is None or not self._merge_thread.is_alive():
            self._merge_thread = threading.Thread(target=self._merge_loop, daemon=True,
                                                  name="merge-" + self.name)
            self._merge_thread.start()
        self._merge_q.put(ridx)

    def _merge_loop(self):
        while True:
            ridx = self._merge_q.get()
            if ridx is None:
                return
            try:
                self._merge_range(ridx)
            except Exception:
                traceback.print_exc()
            finally:
                with self.lock:
                    self.page_ranges[ridx].merge_pending = False

    def merge_now(self, ridx):
        self._merge_range(ridx)

    def _merge_range(self, ridx):
        with self.lock:
            pr = self.page_ranges[ridx]
            start, end, new_tps = pr.merge_pos, pr.tail_records, pr.last_tid
            if end <= start:
                return
        nc = self.num_columns
        latest = {}                                   
        for pidx in range((end - 1) // RECORD_PER_PAGE, start // RECORD_PER_PAGE - 1, -1):
            lo = max(start, pidx * RECORD_PER_PAGE) - pidx * RECORD_PER_PAGE
            hi = min(end, (pidx + 1) * RECORD_PER_PAGE) - pidx * RECORD_PER_PAGE
            key = (ridx, True, pidx)
            with self.lock:
                ps = self.bp.pin(self.name, key)
                try:
                    pg = ps.pages
                    for slot in range(hi - 1, lo - 1, -1):
                        if pg[RID_COLUMN].read(slot) == INVALID_POINTER:
                            continue                  
                        base = pg[BASE_RID_COLUMN].read(slot)
                        if base not in latest:
                            latest[base] = [pg[NUM_META_COLUMNS + c].read(slot) for c in range(nc)]
                finally:
                    self.bp.unpin(self.name, key)
        items = list(latest.items())
        for i in range(0, len(items), 256):
            with self.lock:
                for base, vals in items[i:i + 256]:
                    if base not in self.page_directory:
                        continue                     
                    self._write_cols(base, [(NUM_META_COLUMNS + c, v) for c, v in enumerate(vals)])
        with self.lock:
            pr.tps = new_tps
            pr.merge_pos = end
            self.merges_done += 1

    def shutdown(self):
        t = self._merge_thread
        if t is not None and t.is_alive():
            self._merge_q.put(None)
            t.join()
        self._merge_thread = None

    def __merge(self):
        pass

    def save(self):
        with self.lock:
            meta = {
                "name": self.name, "num_columns": self.num_columns, "key": self.key,
                "next_base_rid": self.next_base_rid, "next_tail_rid": self.next_tail_rid,
                "col_updated": self.col_updated,
                "ranges": [{"base_records": r.base_records, "tail_records": r.tail_records,
                            "merge_pos": r.merge_pos, "tps": r.tps, "last_tid": r.last_tid}
                           for r in self.page_ranges],
            }
            os.makedirs(self.directory, exist_ok=True)
            tmp = os.path.join(self.directory, "meta.json.tmp")
            with open(tmp, "w") as f:
                json.dump(meta, f)
            os.replace(tmp, os.path.join(self.directory, "meta.json"))

            arr = array('q')
            for rid, (r, is_tail, p, s) in self.page_directory.items():
                arr.extend((rid, r, int(is_tail), p, s))
            tmp = os.path.join(self.directory, "directory.bin.tmp")
            with open(tmp, "wb") as f:
                arr.tofile(f)
            os.replace(tmp, os.path.join(self.directory, "directory.bin"))

    @classmethod
    def load(cls, directory, bufferpool):
        with open(os.path.join(directory, "meta.json")) as f:
            meta = json.load(f)
        t = cls(meta["name"], meta["num_columns"], meta["key"], bufferpool, directory)
        t.next_base_rid = meta["next_base_rid"]
        t.next_tail_rid = meta["next_tail_rid"]
        t.col_updated = meta["col_updated"]
        for rm in meta["ranges"]:
            pr = PageRange()
            pr.base_records = rm["base_records"]
            pr.tail_records = rm["tail_records"]
            pr.merge_pos = rm["merge_pos"]
            pr.tps = rm["tps"]
            pr.last_tid = rm["last_tid"]
            t.page_ranges.append(pr)
        path = os.path.join(directory, "directory.bin")
        arr = array('q')
        n = os.path.getsize(path) // arr.itemsize
        with open(path, "rb") as f:
            arr.fromfile(f, n)
        for i in range(0, len(arr), 5):
            rid, r, is_tail, p, s = arr[i:i + 5]
            t.page_directory[rid] = (r, bool(is_tail), p, s)
        # indexes are not persisted: rebuild the key index
        for rid, val in t.scan_latest(t.key):
            t.index.insert_entry(t.key, val, rid)
        return t