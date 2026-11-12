import os 
import shutil
import struct
import threading
from collections import OrderedDict

from lstore.page import Page
from lstore.config import BUFFERPOOL_SIZE, PAGE_SIZE


class PageSet:
    def __init__(self, num_cols):
        self.pages = [Page() for _ in range(num_cols)]
        self.num_records = 0

    def append_row(self, row):
        for page, v in zip(self.pages, row):
            page.write(v)
        slot = self.num_records
        self.num_records += 1
        return slot

    def to_bytes(self):
        return struct.pack('<q', self.num_records) + b''.join(bytes(p.data) for p in self.pages)

    @classmethod
    def from_bytes(cls, raw, num_cols):
        ps = cls(num_cols)
        ps.num_records = struct.unpack_from('<q', raw, 0)[0]
        off = 8
        for p in ps.pages:
            p.data[:] = raw[off:off+PAGE_SIZE]
            p.num_records = ps.num_records
            off += PAGE_SIZE
        return ps

class Frame:
    __slots__ = ('key', 'ps', 'dirty', 'pins')

    def __init__(self, key, ps):
        self.key = key
        self.ps = ps
        self.dirty = False
        self.pins = 0

class BufferPool:
    def __init__(self, capacity=BUFFERPOOL_SIZE):
        self.capacity = capacity
        self.frames = OrderedDict()
        self.tables = {}
        self.lock = threading.RLock()
        self.hits = self.misses = self.evictions = 0

    def register_table(self, name, directory, num_cols):
        os.makedirs(directory, exist_ok=True)
        self.tables[name] = (directory, num_cols)

    def drop_table(self, name):
        with self.lock:
            for fk in [k for k in self.frames if k[0] == name]:
                del self.frames[fk]
            info = self.tables.pop(name, None)
            if info:
                shutil.rmtree(info[0], ignore_errors=True)

    def _path(self, tname, key):
        r, is_tail, idx = key
        return os.path.join(self.tables[tname][0], "%d_%s_%d.pg" % (r, 't' if is_tail else 'b', idx))

    def pin(self, tname, key):
        fk = (tname, key)
        with self.lock:
            fr = self.frames.get(fk)
            if fr is None:
                self.misses += 1
                with open(self._path(tname, key), 'rb') as f:
                    raw = f.read()
                fr = Frame(fk, PageSet.from_bytes(raw, self.tables[tname][1]))
                self.frames[fk] = fr
                fr.pins += 1
                self._evict()
                return fr.ps
            self.hits += 1
            self.frames.move_to_end(fk)
            fr.pins += 1
            return fr.ps

    def new_page(self, tname, key):
        fk = (tname, key)
        with self.lock:
            fr = Frame(fk, PageSet(self.tables[tname][1]))
            fr.dirty = True
            fr.pins = 1
            self.frames[fk] = fr
            self._evict()
            return fr.ps

    def unpin(self, tname, key, dirty=False):
        with self.lock:
            fr = self.frames[(tname, key)]
            fr.pins -= 1
            if dirty:
                fr.dirty = True

    def _write(self, fr):
        path = self._path(fr.key[0], fr.key[1])
        with open(path, 'wb') as f:
            f.write(fr.ps.to_bytes())
        fr.dirty = False

    def _evict(self):
        while len(self.frames) > self.capacity:
            victim = None
            for fk, fr in self.frames.items():      
                if fr.pins == 0:
                    victim = fr
                    break
            if victim is None:
                return                               
            if victim.dirty:
                self._write(victim)
            del self.frames[victim.key]
            self.evictions += 1

    def flush_all(self):
        with self.lock:
            for fr in self.frames.values():
                if fr.dirty:
                    self._write(fr)


