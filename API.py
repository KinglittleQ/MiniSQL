import catalog
import record
# import index
from execption import *
from config import table_schema_file
from buffer_manager import Buffer
import re
import os


def create_table(query):
    query = query.strip('; \n')

    match = re.match(r'^create\s+table\s+([a-z][a-z0-9_]*)\s+\((.+)\)$', query, re.S)
    if match:
        table, attribs = match.groups()
        cols, types, uniques = [], [], []
        n_keys = 0
        key = None
        for a in attribs.strip(',\n').split(','):
            a = a.strip()
            primary_key = re.match(r'primary\s+key\s*\((.+)\)', a, re.S)
            if primary_key:
                key = primary_key.group(1)
                n_keys += 1
            else:
                a = a.split(' ')
                col, t = a[0].strip(), a[1].strip()
                if len(a) >= 3 and a[2].strip() == 'unique':
                    uniques.append(True)
                else:
                    uniques.append(False)
                cols.append(col)
                types.append(t)
        # print(cols, types, key)
        if n_keys > 1:
            raise MiniSQLError('Multiple primary keys')
        else:
            catalog.create_table(table.strip(), cols, types, key, uniques)
            # record.create_table(table, cols, types, key)
    else:
        raise MiniSQLSyntaxError('''Syntax Error in '{}' '''.format(query))


def create_index(query, buf):
    query = query.strip('; \n')
    match = re.match(r'^create\s+index\s+(.+)\s+on\s+(.+)\s*\((.+)\)$', query, re.S)
    if match:
        index_name = match.group(1).strip()
        table = match.group(2).strip()
        col = match.group(3).strip()
        catalog.create_index(index_name, table, col, buf)
    else:
        raise MiniSQLSyntaxError('''Syntax Error in '{}' '''.format(query))


def insert(query, buf):
    query = query.strip('; \n')
    match = re.match(r'^insert\s+into\s+(.+)\s+values\s*\((.+)\)$', query, re.S)
    if match:
        table = match.group(1).strip()
        values = []
        for v in match.group(2).split(','):
            values.append(v.strip())
        record.insert(table, values, buf)
    else:
        raise MiniSQLSyntaxError('''Syntax Error in '{}' '''.format(query))


def select(query, buf):
    query = query.strip('; \n')
    match = re.match(r'^select\s+(.+)\s+from\s+(.*)\s+where\s+(.+)$', query, re.S)
    if match:
        cols, table, condition = match.groups()
        # print(table)
    else:
        match = re.match(r'^select\s+(.*)\s+from\s+(.*)$', query, re.S)
        if match:
            cols, table = match.groups()
            condition = None
        else:
            raise MiniSQLSyntaxError('Syntax Error in select')
    if cols != '*':
        cols = cols.strip('()').split(',')
        cols = [c.strip() for c in cols]
    return record.select(cols, table.strip(), condition, buf)


def delete(query, buf):
    query = query.strip('; \n')
    match = re.match(r'^delete\s+from\s+(.+)\s+where\s+(.+)$', query, re.S)
    if match:
        table, condition = match.groups()
        condition = condition.strip()
    else:
        match = re.match(r'^delete\s+from\s+(.+)', query, re.S)
        if match:
            table = match.group(1)
            condition = None
        else:
            raise MiniSQLSyntaxError('Syntax Error in select')
    record.delete(table.strip(), condition, buf)


def drop_table(query, buf):
    query = query.strip('; \n')
    match = re.match(r'^drop\s+table\s+(.+)$', query, re.S)
    if match:
        table = match.group(1).strip()
        # record.drop_table(table)
        catalog.drop_table(table, buf)
    else:
        raise MiniSQLSyntaxError('''Syntax Error in '{}' '''.format(query))


def drop_index(query, buf):
    query = query.strip('; \n')
    match = re.match(r'^drop\s+index\s+(.+)$', query, re.S)
    if match:
        index_name = match.group(1).strip()
        catalog.drop_index(index_name)
    else:
        raise MiniSQLSyntaxError('''Syntax Error in '{}' '''.format(query))


def clear_all(buf):
    buf.close()

    os.remove(table_schema_file)

    dirs = ['DB/index', 'DB/memory']

    for d in dirs:
        for path in os.listdir(d):
            os.remove(os.path.join(d, path))

    buf = Buffer()
    return buf


if __name__ == '__main__':
    # sql = '''drop table student;'''
    buf = Buffer()
    sql = '''insert into student values (3229555278, 'eng', 187);'''
    create_sql = '''create table student (ID char(20), name char(20) unique, height int, primary key (ID));'''
    select_sql = '''select * from student where ID = 3189555278;'''
    delete_sql = '''delete from student where name = 'han' '''
    create_index_sql = '''create index sid on student (ID);'''
    # print(sql)
    # create_table(create_sql)
    # create_index(create_index_sql, buf)
    # insert(sql, buf)
    # delete(delete_sql, buf)
    print(select(select_sql, buf))
    buf.close()
