create database sol;
open sol;

create table planets (name string, mass int, atmosphere bool);
insert into planets (name, mass, atmosphere) values (null, 20, false), ("Mars", null, true), ("Venus", 10, null), ("Earth", 10, true);
insert into planets (name, mass, atmosphere) values (null, null, null), ("Jupiter", null, null), (null, null, false);
drop table planets;

close sol;
drop database sol;
exit;
