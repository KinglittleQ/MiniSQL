import pickle
import os
from config import *
from utils import *
from execption import *
import random


class Block():

    def __init__(self, ID, size, table=None):
        self.block_size = size
        self.dirty = False
        self.ID = ID
        self.memory = []
        self.table = table

    def append(self, record):
        if self.size() + get_size(record) <= self.block_size:
            self.memory.append(record)
            self.dirty = True
            return True
        else:
            return False

    def data(self):
        return self.memory

    def clear(self):
        self.memory = []
        self.dirty = False

    def set_dirty(self, dirty):
        self.dirty = dirty

    def size(self):
        return self.get_size(self.memory)

    def usable_size(self):
        return self.block_size - self.size()

    @staticmethod
    def get_size(obj):
        return len(pickle.dumps(obj, protocol=protocol))


class Buffer():

    def __init__(self):
        n_blocks = 10
        self.n_blocks = n_blocks  # number of blocks in buffer
        self.header = None  # information about all blocks
        self.blocks = []  # blocks in buffer
        self.blocks_id = []  # blocks id in buffer

        if os.path.exists(header_path):
            self.header = load(header_path)
        else:
            header = []
            for i in range(TOTAL_BLOCK):
                b = Block(i, BLOCK_SIZE)
                dump(b, block_path.format(i))
                header.append({'table': None, 'usable_size': b.usable_size()})

            self.header = header

        self.blocks_id = list(range(n_blocks))
        self.blocks = [load(block_path.format(i)) for i in self.blocks_id]

    def get_block(self, idx):
        if idx in self.blocks_id:
            b = self.blocks[self.blocks_id.index(idx)]
            b.set_dirty(True)
            return b
        else:
            target_block = load(block_path.format(idx))
            n = random.randint(0, self.n_blocks - 1)
            self.write_back(n)
            self.blocks[n] = target_block
            self.blocks_id[n] = idx
            target_block.set_dirty(True)
            return target_block

    def update_header(self, block_id, block, table):
        origin_table = self.header[block_id]['table']
        if origin_table and table and table != self.header[block_id]['table']:
            raise MiniSQLError('Can\'t write different tables into one block')
        else:
            self.header[block_id]['table'] = table
            self.header[block_id]['usable_size'] = block.size()

    def write_back(self, idx=None):
        if idx is not None:
            dump(self.blocks[idx], block_path.format(self.blocks_id[idx]))
            self.blocks[idx].set_dirty(False)
        else:
            for i in range(self.n_blocks):
                dump(self.blocks[i], block_path.format(self.blocks_id[i]))
                self.blocks[i].set_dirty(False)

    def close(self):
        dump(self.header, header_path)
        self.write_back()


if __name__ == '__main__':
    buf = Buffer()
    # for i in buf.header:
    #     print(i)
    # for i in range(100):
    #     b = buf.get_block(i)
    #     print(b.dirty)

    for i, b in enumerate(buf.blocks):
        print(buf.header[i])
        # buf.update_header(i, b, None)

    buf.close()
