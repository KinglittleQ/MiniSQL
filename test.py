from API import *


create_sql = ''' create table student (
    ID char(20),
    name char(10),
    age int,
    height float,
    primary key (ID)
); '''

insert_sql = ''' insert into student values (
    3160115278,
    'qiu',
    20,
    177.5
); '''

delete_sql = ''' delete from student '''

select_sql = ''' select * from student; '''

# create_table(create_sql)
# insert(insert_sql)
# delete(delete_sql)
print(select(select_sql))

