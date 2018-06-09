import pickle
import re
from execption import *
from config import *
# from interpreter import buf
from utils import *


def insert(table, values, buf):

    tb = buf.get_block(0)

    schemas = pickle.load(open(table_schema_file, 'rb'))
    if table in schemas:
        schema = schemas[table]
    else:
        raise MiniSQLError('table {} does not exit'.format(table))

    record = []
    if len(values) == len(schema['types']):
        for t, v in zip(schema['types'], values):
            record.append(convert_to(v, t))
    else:
        raise MiniSQLError('values does not mach table schema')

    record_size = get_size(record)
    table_blocks = []
    size_match = []
    for i, b in enumerate(buf.header):
        if b['table'] == table:
            table_blocks.append(i)
            size_match.append(b['usable_size'] >= record_size)

    if len(table_blocks) == 0 or not any(size_match):
        for i, b in enumerate(buf.header):
            if b['table'] is None:
                break

        block = buf.get_block(i)
        block.append(record)

        pos = len(block.data()) - 1
        ptr = i * MAX_RECORDS_PER_BLOCK + pos

        buf.update_header(i, block, table)
    else:
        uniques = schema['uniques']
        indices = []
        for i, u in enumerate(uniques):
            if u:
                indices.append(i)

        key = schema['primary_key']
        if key is not None:
            indices.append(schema['cols'].index(key))

        if len(indices) > 0:
            for key in indices:
                for idx in table_blocks:
                    block = buf.get_block(idx)
                    for r in block.data():
                        if r[key] == record[key]:
                            raise MiniSQLError('duplicate keys confilct: {}'.format(r[key]))

        for i, m in zip(table_blocks, size_match):
            if m:
                block = buf.get_block(i)
                block.append(record)
                buf.update_header(i, block, table)

                pos = len(block.data()) - 1
                ptr = i * MAX_RECORDS_PER_BLOCK + pos

                break

    if len(schema['index']) != 0:
        for col, name in schema['index']:
            tree = load(index_file_prefix.format(name))
            idx = schema['cols'].index(col)
            tree.insert(record[idx], ptr)
            dump(tree, index_file_prefix.format(name))

    # log('insert {} into table {}'.format(record, table))


def delete(table, condition, buf):

    schemas = pickle.load(open(table_schema_file, 'rb'))
    if table in schemas:
        schema = schemas[table]
    else:
        raise MiniSQLError('table {} does not exit'.format(table))
    schema_cols = schema['cols']
    schema_types = schema['types']

    # get all blocks with table
    table_blocks = []
    for i, b in enumerate(buf.header):
        if b['table'] == table:
            table_blocks.append(i)

    if condition is not None:
        exps = condition.split('and')
        for exp in exps:
            exp = exp.strip()
            match = re.match(r'^([A-Za-z0-9_]+)\s*([<>=]+)\s*(.+)$', exp, re.S)
            if match and len(match.groups()) == 3:
                col, op, value = match.groups()
                for i in table_blocks:
                    b = buf.get_block(i)
                    records = b.data()
                    records = select_filter(records, col, op, value, schema_cols, schema_types, reverse=True)
                    b.memory = records
                    if len(records) == 0:
                        buf.update_header(i, b, None)
            else:
                raise MiniSQLSyntaxError('Illegal condition: {}'.format(exp))
    else:
        # table_records = []
        for i in table_blocks:
            b = buf.get_block(i)
            b.memory = []
            buf.update_header(i, b, None)


def drop_table(table, buf):
    delete(table, None, buf)


def select(cols, table, condition, buf):

    schemas = pickle.load(open(table_schema_file, 'rb'))
    if table in schemas:
        schema = schemas[table]
    else:
        raise MiniSQLError('table {} does not exit'.format(table))
    schema_cols = schema['cols']
    schema_types = schema['types']
    index = schema['index']

    index_cols = []
    trees = []
    if len(index) != 0:
        for col, index_name in index:
            index_cols.append(col)
            tree = load(index_file_prefix.format(index_name))
            trees.append(tree)

    return_records = []

    table_blocks = []
    for i, b in enumerate(buf.header):
        if b['table'] == table:
            table_blocks.append(i)

    # select from all blocks
    if condition is not None:
        exps = condition.split('and')

        if len(index) != 0 and len(exps) == 1:
            exp = exps[0].strip()
            match = re.match(r'^([A-Za-z0-9_]+)\s*([<>=]+)\s*(.+)$', exp, re.S)

            if match and len(match.groups()) == 3:
                col, op, value = match.groups()

                if op == '=' and col in index_cols:
                    col_index = index_cols.index(col)
                    tree = trees[col_index]
                    value = convert_to(value, schema_types[col_index])
                    ptrs = tree.search(value)
                    if isinstance(ptrs, list):
                        for p in ptrs:
                            block_id = p // MAX_RECORDS_PER_BLOCK
                            pos = p % MAX_RECORDS_PER_BLOCK
                            b = buf.get_block(block_id)
                            return_records.append(b.data()[pos])
                    else:
                        block_id = ptrs // MAX_RECORDS_PER_BLOCK
                        pos = ptrs % MAX_RECORDS_PER_BLOCK
                        b = buf.get_block(block_id)
                        return_records.append(b.data()[pos])
                else:
                    return_records = _select_without_index(table_blocks, buf, exps, schema_cols, schema_types)

        else:
            return_records = _select_without_index(table_blocks, buf, exps, schema_cols, schema_types)

    else:
        for i in table_blocks:
            b = buf.get_block(i)
            records = b.data()
            return_records += records

    # select cols
    if cols != '*':
        indices = [schema_cols.index(c) for c in cols]
        return_records = [[r[i] for i in indices] for r in return_records]
        return_cols = cols
    else:
        return_cols = schema_cols

    log('select from {}'.format(table))

    return return_records, return_cols


def _select_without_index(table_blocks, buf, exps, schema_cols, schema_types):
    return_records = []
    for i in table_blocks:
        b = buf.get_block(i)
        records = b.data()

        for exp in exps:
            exp = exp.strip()
            match = re.match(r'^([A-Za-z0-9_]+)\s*([<>=]+)\s*(.+)$', exp, re.S)
            if match and len(match.groups()) == 3:
                col, op, value = match.groups()
                records = select_filter(records, col, op, value, schema_cols, schema_types)
            else:
                raise MiniSQLSyntaxError('Illegal condition: {}'.format(exp))
        return_records += records
    return return_records


def convert_to(v, t):
    if t == 0:
        return int(v)
    elif t == -1:
        return float(v)
    else:
        return v.strip("'")


def select_filter(records, col, op, value, schema_cols, schema_types, reverse=False):
    col_index = schema_cols.index(col)
    value = convert_to(value, schema_types[col_index])

    def f1(v):
        return v > value

    def f2(v):
        return v < value

    def f3(v):
        return v == value

    def f4(v):
        return v >= value

    def f5(v):
        return v <= value

    def f6(v):
        return v != value

    funcs = {'>': f1, '<': f2, '=': f3, '>=': f4, '<=': f5, '<>': f6}
    if op in funcs:
        f = funcs[op]
    else:
        raise MiniSQLError('Illegal operator: {}'.format(op))

    new_records = []
    for r in records:
        satisfy = f(r[col_index])
        if reverse:
            satisfy = not satisfy
        if satisfy:
            new_records.append(r)

    return new_records


if __name__ == '__main__':
    print(convert_to("'hello'", 20))
