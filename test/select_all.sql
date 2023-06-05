create database sol;
open sol;

create table planets (id int key, name string, mass int, atmosphere bool);
insert into planets (id, name, mass, atmosphere) values (1, "Neptune", 20, false), (2, "Mars", 10, true), (3, "Venus", 10, false);
select * from planets;
drop table planets;

close sol;
drop database sol;
exit;
