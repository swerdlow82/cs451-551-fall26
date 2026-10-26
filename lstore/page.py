import struct
from lstore.config import PAGE_SIZE, VALUE_SIZE, RECORD_PER_PAGE

class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        return self.num_records < RECORD_PER_PAGE

    

    def write(self, value):
        slot = self.num_records
        struct.pack_into('<q', self.data, slot * VALUE_SIZE, value)
        self.num_records += 1
        return slot

    def read(self, slot):
        return struct.unpack_from('<q', self.data, slot * VALUE_SIZE)[0]


    def overwrite(self, slot, value):
        struct.pack_into('<q', self.data, slot * VALUE_SIZE, value)

