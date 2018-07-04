create table people (
    ID char(20),
    name char(20),
    age int,
    height float,
    primary key (ID)
);

insert into people values (
    3150659278,
    'dong',
    20,
    177.5
);

insert into people values (
    3191115278,
    'han',
    22,
    187.5
);

insert into people values (
    3191118888,
    'chen',
    19,
    187.5
);

insert into people values (
    3179615278,
    'wei',
    18,
    187.5
);


select * from people;
delete from people where age > 20;
select * from people;
delete from people;
select * from people;

select * from people
    where age > 20;


create index sid on people (ID);


select * from people;

drop table people;

