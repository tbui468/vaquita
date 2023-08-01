create database sol;
open sol;

create table planets (id int8 key, name text, mass int8, atmosphere bool);
insert into planets (id, name, mass, atmosphere) values (1, "Neptune", 20, false), (2, "Mars", 10, true), (3, "Venus", 10, false), (4, "Earth", 10, true);
insert into planets (id, name, mass, atmosphere) values (5, "Mercury", 5, true), (6, "Jupiter", 100, true), (7, "Saturn", 323, false);
select * from planets limit 5;
select * from planets order by id desc limit 5;
drop table planets;

close sol;
drop database sol;
exit;
