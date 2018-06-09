import pickle
import re
import os
import record
from execption import *
from config import *
from utils import *
from index import BPlusTree


# protocol = pickle.DEFAULT_PROTOCOL
# table_schema_file = 'DB/catalog/table_schema.pkl'


def create_table(table, cols, types, key, uniques):
    try:
        schemas = pickle.load(open(table_schema_file, 'rb'))
    except FileNotFoundError:
        schemas = {}
    if table not in schemas:
        digit_types = [convert_type(t) for t in types]
        schema = {'cols': cols, 'types': digit_types, 'primary_key': key, 'uniques': uniques, 'index': []}
        schemas[table] = schema
        with open(table_schema_file, 'wb') as f:
            pickle.dump(schemas, f, protocol=protocol)

        log('create {} schema {} with {}, primary_key={}'.format(table, cols, types, key))
        # record.create_table(table)
    else:
        log('table {} all ready exits'.format(table))


def create_index(index_name, table, col, buf):
    schemas = load(table_schema_file)
    if table in schemas:
        schema = schemas[table]
    else:
        raise MiniSQLError('table {} does not exit'.format(table))

    index_file = index_file_prefix.format(index_name)
    if os.path.exists(index_file):
        raise MiniSQLError('index {} all ready exits'.format(index_name))

    tree = BPlusTree(TREE_ORDER)

    table_blocks = []
    for i, b in enumerate(buf.header):
        if b['table'] == table:
            table_blocks.append(i)

    idx = schema['cols'].index(col)

    for block_idx in table_blocks:
        b = buf.get_block(block_idx)
        records = b.data()

        for i, r in enumerate(records):
            ptr = block_idx * MAX_RECORDS_PER_BLOCK + i
            tree.insert(r[idx], ptr)

    schemas[table]['index'].append([col, index_name])

    dump(schemas, table_schema_file)
    dump(tree, index_file)


def drop_index(index_name):
    index_file = index_file_prefix.format(index_name)
    if not os.path.exists(index_file):
        raise MiniSQLError('index {} doesn\'t exit'.format(index_name))
    os.remove(index_file)

    schemas = load(table_schema_file)
    for table in schemas:
        s = schemas[table]
        index = s['index']
        if index:
            for i, pair in enumerate(index):
                if pair[1] == index_name:
                    s['index'].pop(i)
                    break
    dump(schemas, table_schema_file)


def drop_table(table, buf):
    try:
        schemas = pickle.load(open(table_schema_file, 'rb'))
    except FileNotFoundError:
        schemas = {}
    if table in schemas:
        record.drop_table(table, buf)

        # drop index
        index = schemas[table]['index']
        if index:
            for col, name in index:
                drop_index(name)

        schemas.pop(table)
        with open(table_schema_file, 'wb') as f:
            pickle.dump(schemas, f, protocol=protocol)

        log('drop table {}'.format(table))


def convert_type(t):
    if t == 'int':
        return 0
    elif t == 'float':
        return -1
    else:
        match = re.match(r'^char\((\d+)\)$', t, re.S)
        if match:
            n = int(match.group(1))
            if n >= 1 and n <= 255:
                return n
            else:
                raise MiniSQLError('char(n): n is out of range [1, 255]')
        else:
            raise MiniSQLSyntaxError('Error in type {}'.format(t))


if __name__ == '__main__':
    print(convert_type('char(255)'))
