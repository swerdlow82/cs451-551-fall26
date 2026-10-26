
from bisect import bisect_left, bisect_right, insort


class ColumnIndex:
    def __init__(self):
        self.map = {}        # value -> set of RIDs
        self.sorted = []     # sorted distinct values

    def add(self, value, rid):
        s = self.map.get(value)
        if s is None:
            self.map[value] = {rid}
            insort(self.sorted, value)
        else:
            s.add(rid)

    def remove(self, value, rid):
        s = self.map.get(value)
        if s is None:
            return
        s.discard(rid)
        if not s:
            del self.map[value]
            i = bisect_left(self.sorted, value)
            if i < len(self.sorted) and self.sorted[i] == value:
                del self.sorted[i]

    def get(self, value):
        return list(self.map.get(value, ()))

    def range(self, begin, end):
        lo = bisect_left(self.sorted, begin)
        hi = bisect_right(self.sorted, end)
        out = []
        for v in self.sorted[lo:hi]:
            out.extend(self.map[v])
        return out


class Index:

    def __init__(self, table):
        # One index for each table. All empty initially, except the key column.
        self.table = table
        self.indices = [None] * table.num_columns
        self.indices[table.key] = ColumnIndex()

    def has_index(self, column):
        return self.indices[column] is not None

    def locate(self, column, value):
        """Returns the (base) RIDs of all records with the given value on column."""
        idx = self.indices[column]
        if idx is None:
            return None          # caller must fall back to a scan
        return idx.get(value)

    def locate_range(self, begin, end, column):
        """Returns the RIDs of all records with values in [begin, end]."""
        idx = self.indices[column]
        if idx is None:
            return None
        return idx.range(begin, end)

    # --- maintenance hooks used by Table -------------------------------
    def insert_entry(self, column, value, rid):
        if self.indices[column] is not None:
            self.indices[column].add(value, rid)

    def remove_entry(self, column, value, rid):
        if self.indices[column] is not None:
            self.indices[column].remove(value, rid)

    def create_index(self, column_number):
        if self.indices[column_number] is not None:
            return
        idx = ColumnIndex()
        self.indices[column_number] = idx
        for rid, val in self.table.scan_latest(column_number):
            idx.add(val, rid)

    def drop_index(self, column_number):
        if column_number == self.table.key:
            return               # key index is mandatory
        self.indices[column_number] = None