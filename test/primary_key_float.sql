create database sol;
open sol;

create table planets (id float8 key, name text, mass int8, atmosphere bool);
insert into planets (id, name, mass, atmosphere) values 
    (3.0, "Mars", 10, true), 
    (-2.0, "Venus", 10, false),
    (1.0, "Neptune", 20, false);
select * from planets;
drop table planets;

close sol;
drop database sol;
exit;
