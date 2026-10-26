from time import time
from lstore.index import Index
from lstore.page import Page
from lstore.config import *


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

    def __getitem__(self, i):          # lets r[i] work (used by Query.increment)
        return self.columns[i]

    def __repr__(self):
        return "Record(rid=%s, key=%s, columns=%s)" % (self.rid, self.key, self.columns)


class PageSet:
    """A 'logical' page: one physical Page per column (meta + data)."""

    def __init__(self, num_phys_cols):
        self.pages = [Page() for _ in range(num_phys_cols)]

    def has_capacity(self):
        return self.pages[0].has_capacity()


class PageRange:
    """Fixed number of base pages plus a growing list of tail pages."""

    def __init__(self, num_phys_cols):
        self.num_phys_cols = num_phys_cols
        self.base_pages = []
        self.tail_pages = []

    def base_full(self):
        return (len(self.base_pages) >= BASE_PAGES_PER_RANGE
                and not self.base_pages[-1].has_capacity())

    def current_base(self):
        if not self.base_pages or not self.base_pages[-1].has_capacity():
            self.base_pages.append(PageSet(self.num_phys_cols))
        return len(self.base_pages) - 1

    def current_tail(self):
        if not self.tail_pages or not self.tail_pages[-1].has_capacity():
            self.tail_pages.append(PageSet(self.num_phys_cols))
        return len(self.tail_pages) - 1


class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.num_phys_cols = NUM_META_COLUMNS + num_columns
        # RID -> (page_range_idx, is_tail, page_idx, slot)
        self.page_directory = {}
        self.page_ranges = []
        self.next_rid = 1            # one counter => base and tail RIDs never collide
        self.col_updated = [False] * num_columns   # has any record ever changed this column?
        self.index = Index(self)

    # ------------------------------------------------------------ helpers
    def _new_rid(self):
        rid = self.next_rid
        self.next_rid += 1
        return rid

    def _write_row(self, pageset, row):
        slot = None
        for page, v in zip(pageset.pages, row):
            slot = page.write(v)
        return slot

    def _read_col(self, rid, phys_col):
        r, is_tail, p, s = self.page_directory[rid]
        pr = self.page_ranges[r]
        ps = (pr.tail_pages if is_tail else pr.base_pages)[p]
        return ps.pages[phys_col].read(s)

    def _write_col(self, rid, phys_col, value):
        r, is_tail, p, s = self.page_directory[rid]
        pr = self.page_ranges[r]
        ps = (pr.tail_pages if is_tail else pr.base_pages)[p]
        ps.pages[phys_col].overwrite(s, value)

    # ------------------------------------------------------------ insert
    def insert_record(self, columns):
        """columns: list of ints (len == num_columns). Returns the new RID."""
        if not self.page_ranges or self.page_ranges[-1].base_full():
            self.page_ranges.append(PageRange(self.num_phys_cols))
        ridx = len(self.page_ranges) - 1
        pr = self.page_ranges[ridx]
        pidx = pr.current_base()
        rid = self._new_rid()
        row = [NULL_POINTER, rid, int(time()), 0] + list(columns)
        slot = self._write_row(pr.base_pages[pidx], row)
        self.page_directory[rid] = (ridx, False, pidx, slot)
        for c in range(self.num_columns):
            self.index.insert_entry(c, columns[c], rid)
        return rid

    # ------------------------------------------------------------ read
    def _resolve(self, base_rid, relative_version=0):
        """RID of the record holding the requested version (base or tail)."""
        cur = self._read_col(base_rid, INDIRECTION_COLUMN)
        if cur == NULL_POINTER:
            return base_rid
        steps = -relative_version if relative_version < 0 else 0
        for _ in range(steps):
            prev = self._read_col(cur, INDIRECTION_COLUMN)   # previous tail, or base
            cur = prev
            if cur == base_rid:
                break
        return cur

    def read_record(self, base_rid, projected, relative_version=0):
        """Returns a list with projected values (None where not projected)."""
        src = self._resolve(base_rid, relative_version)
        return [self._read_col(src, NUM_META_COLUMNS + c) if projected[c] else None
                for c in range(self.num_columns)]

    def read_value(self, base_rid, column, relative_version=0):
        src = self._resolve(base_rid, relative_version)
        return self._read_col(src, NUM_META_COLUMNS + column)

    def exists(self, rid):
        return rid in self.page_directory

    # ------------------------------------------------------------ update
    def update_record(self, base_rid, columns):
        """columns: list with None for unchanged columns. Cumulative tail records:
        each tail holds the latest value for ALL columns."""
        latest_rid = self._resolve(base_rid, 0)
        cur_vals = [self._read_col(latest_rid, NUM_META_COLUMNS + c)
                    for c in range(self.num_columns)]
        base_schema = self._read_col(base_rid, SCHEMA_ENCODING_COLUMN)
        new_vals = list(cur_vals)
        for c, v in enumerate(columns):
            if v is not None:
                new_vals[c] = v
                self.col_updated[c] = True
                base_schema |= (1 << c)
        prev_ptr = self._read_col(base_rid, INDIRECTION_COLUMN)
        if prev_ptr == NULL_POINTER:
            prev_ptr = base_rid                       # first tail points back to base

        ridx = self.page_directory[base_rid][0]
        pr = self.page_ranges[ridx]
        pidx = pr.current_tail()
        trid = self._new_rid()
        row = [prev_ptr, trid, int(time()), base_schema] + new_vals
        slot = self._write_row(pr.tail_pages[pidx], row)
        self.page_directory[trid] = (ridx, True, pidx, slot)

        self._write_col(base_rid, INDIRECTION_COLUMN, trid)
        self._write_col(base_rid, SCHEMA_ENCODING_COLUMN, base_schema)

        for c in range(self.num_columns):
            if new_vals[c] != cur_vals[c]:
                self.index.remove_entry(c, cur_vals[c], base_rid)
                self.index.insert_entry(c, new_vals[c], base_rid)

    # ------------------------------------------------------------ delete
    def delete_record(self, base_rid):
        vals = self.read_record(base_rid, [1] * self.num_columns)
        for c in range(self.num_columns):
            self.index.remove_entry(c, vals[c], base_rid)
        # invalidate the tail chain, then the base record
        cur = self._read_col(base_rid, INDIRECTION_COLUMN)
        while cur != NULL_POINTER and cur != base_rid:
            nxt = self._read_col(cur, INDIRECTION_COLUMN)
            self._write_col(cur, RID_COLUMN, INVALID_RID)
            del self.page_directory[cur]
            cur = nxt
        self._write_col(base_rid, RID_COLUMN, INVALID_RID)
        del self.page_directory[base_rid]

    # ------------------------------------------------------------ scans
    def scan_latest(self, column):
        """Yields (base_rid, latest value of column) for every live base record."""
        for rid, (r, is_tail, p, s) in list(self.page_directory.items()):
            if not is_tail:
                yield rid, self.read_value(rid, column)

    def __merge(self):
        # Implemented in milestone 2.
        pass