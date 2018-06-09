from enum import Enum
from execption import MiniSQLSyntaxError
from API import *
from buffer_manager import *


MiniSQLType = Enum('MiniSQLType', ('CREATE_TABLE', 'INSERT', 'DROP_TABLE', 'CREATE_INDEX',
                                   'DROP_INDEX', 'SELECT', 'DELETE', 'QUIT', 'EXECFILE', 'CLEAR'))


def judge_type(query):
    query = query.lower().strip(';\n ').split()
    if query[0] == 'insert':
        return MiniSQLType.INSERT
    elif query[0] == 'select':
        return MiniSQLType.SELECT
    elif query[0] == 'delete':
        return MiniSQLType.DELETE
    elif query[0] in ['quit', 'exit']:
        return MiniSQLType.QUIT
    elif query[0] == 'execfile':
        return MiniSQLType.EXECFILE
    elif query[0] == 'create':
        if query[1] == 'table':
            return MiniSQLType.CREATE_TABLE
        elif query[1] == 'index':
            return MiniSQLType.CREATE_INDEX
    elif query[0] == 'drop':
        if query[1] == 'table':
            return MiniSQLType.DROP_TABLE
        elif query[1] == 'index':
            return MiniSQLType.DROP_INDEX
    elif query[0] == 'clear':
        return MiniSQLType.CLEAR

    raise MiniSQLSyntaxError('Error Type')


def interpret(query, buf):
    query_type = judge_type(query)
    # query = query.strip('; \n')
    if query_type == MiniSQLType.CREATE_TABLE:
        return create_table(query)
    elif query_type == MiniSQLType.CREATE_INDEX:
        return create_index(query, buf)
    elif query_type == MiniSQLType.INSERT:
        return insert(query, buf)
    elif query_type == MiniSQLType.SELECT:
        return select(query, buf)
    elif query_type == MiniSQLType.DELETE:
        return delete(query, buf)
    elif query_type == MiniSQLType.DROP_INDEX:
        return drop_index(query, buf)
    elif query_type == MiniSQLType.DROP_TABLE:
        return drop_table(query, buf)
    elif query_type == MiniSQLType.QUIT:
        return 0
    elif query_type == MiniSQLType.EXECFILE:
        return execfile(query, buf)
    elif query_type == MiniSQLType.CLEAR:
        return clear_all(buf)


def execfile(query, buf):
    # file = query.strip(';').split()[-1]
    file = query.strip('; \n').split()[-1]
    with open(file, 'r') as f:
        query = ''
        for i, line in enumerate(f.readlines()):
            query += line
            line = line.strip()
            if line and line[-1] == ';':
                try:
                    ret = interpret(query, buf)
                    if isinstance(ret, Buffer):
                        buf = Buffer
                    if ret == 0:
                        break
                    elif isinstance(ret, (list, tuple)):
                        print_table(*ret)
                except MiniSQLError as e:
                    print('In line {}: {}'.format(i + 1, e))
                    break
                query = ''


def print_table(records, cols):
    col_width = 15
    n_cols = len(cols)
    bound = '+' + ''.join(['-' for i in range(n_cols * col_width + n_cols - 1)]) + '+'
    content = '|' + ''.join(['{:^15}|' for i in range(n_cols)])
    print(bound)
    print(content.format(*cols))
    if records:
        for r in records:
            print(bound)
            print(content.format(*r))
    print(bound)


def main():
    query = ''
    buf = Buffer()

    while True:
        print('MiniSQL->', end=' ')
        cmd = input()
        query += cmd + ' '
        cmd = cmd.strip()
        if cmd and cmd[-1] == ';':
            # print(query)
            try:
                ret = interpret(query, buf)
                if ret == 0:
                    break
                elif isinstance(ret, (list, tuple)):
                    print_table(*ret)
            except MiniSQLError as e:
                print(e)
            query = ''

    buf.close()


if __name__ == '__main__':
    main()
