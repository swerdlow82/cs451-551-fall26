from lstore.table import Table, Record
from lstore.index import Index


class Query:
    def __init__(self, table):
        self.table = table
        

    
    def delete(self, primary_key):
        try:
            rids = self.table.index.locate(self.table.key, primary_key)
            if not rids:
                return False
            self.table.delete_record(rids[0])
            return True
        except Exception:
            return False
    
    
    def insert(self, *columns):
        try:
            if len(columns) != self.table.num_columns or any(c is None for c in columns):
                return False
            if self.table.index.locate(self.table.key, columns[self.table.key]):
                return False
            self.table.insert_record(list(columns))
            return True
        except Exception:
            return False

    
    
    def select(self, search_key, search_key_index, projected_columns_index):
        return self.select_version(search_key, search_key_index, projected_columns_index, 0)

    
    def select_version(self, search_key, search_key_index, projected_columns_index, relative_version):
        try:
            t = self.table
            rids = t.index.locate(search_key_index, search_key)
            if rids is None:                      
                rids = [rid for rid, v in t.scan_latest(search_key_index)
                        if t.read_value(rid, search_key_index, relative_version) == search_key]
            elif relative_version != 0 and t.col_updated[search_key_index]:
                rids = [rid for rid, _ in t.scan_latest(search_key_index)
                        if t.read_value(rid, search_key_index, relative_version) == search_key]
            out = []
            for rid in rids:
                cols = t.read_record(rid, projected_columns_index, relative_version)
                key = t.read_value(rid, t.key, relative_version)
                out.append(Record(rid, key, cols))
            return out
        except Exception:
            return False


    def update(self, primary_key, *columns):
        try:
            t = self.table
            if len(columns) != t.num_columns:
                return False
            rids = t.index.locate(t.key, primary_key)
            if not rids:
                return False
            new_key = columns[t.key]
            if new_key is not None and new_key != primary_key \
                    and t.index.locate(t.key, new_key):
                return False                      
            t.update_record(rids[0], list(columns))
            return True
        except Exception:
            return False

    

    def sum(self, start_range, end_range, aggregate_column_index):
        return self.sum_version(start_range, end_range, aggregate_column_index, 0)

    
   
    def sum_version(self, start_range, end_range, aggregate_column_index, relative_version):
        try:
            t = self.table
            rids = t.index.locate_range(start_range, end_range, t.key)
            if not rids:
                return False
            return sum(t.read_value(rid, aggregate_column_index, relative_version)
                    for rid in rids)
        except Exception:
            return False

    
    
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
