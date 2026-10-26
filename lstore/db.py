from lstore.table import Table


class Database():

    def __init__(self):
        self.tables = {}

    # Not required for milestone1
    def open(self, path):
        pass

    def close(self):
        pass

    def create_table(self, name, num_columns, key_index):
        if name in self.tables:
            raise ValueError("table %s already exists" % name)
        table = Table(name, num_columns, key_index)
        self.tables[name] = table
        return table

    def drop_table(self, name):
        return self.tables.pop(name, None) is not None

    def get_table(self, name):
        return self.tables.get(name)