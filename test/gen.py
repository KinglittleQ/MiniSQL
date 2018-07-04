f = open('04.sql', 'w')
sql = '''insert into student values ({:0>10}, 'deng', 18);\n'''

create_sql = '''create table student (
    ID char(20),
    name char(20),
    age int
);\n'''

f.write(create_sql)

for i in range(100000):
    f.write(sql.format(i))

f.close()
