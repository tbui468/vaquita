create database sol;
open sol;

create table planets (id int8 key, name text, moons int8, gravity float8, atmosphere bool);
insert into planets (id, name, moons, gravity, atmophere) values (1, "Mars", 2, 2.23, true);
insert into planets (id, name, moons, gravity, atmospere) values (2, "Earth", 1, 82.32, false), (3, "Venus", 0, -2.32, true);
drop table planets;

close sol;
drop database sol;
exit;
