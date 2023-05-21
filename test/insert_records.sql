create database sol;
open sol;

create table planets (name string, moons int, gravity float, atmosphere bool);
insert into planets (name, moons, gravity, atmophere) values ("Mars", 2, 2.23, true);
insert into planets (name, moons, gravity, atmospere) values ("Earth", 1, 82.32, false), ("Venus", 0, -2.32, true);
insert into planets (gravity, atmosphere, name, moons) values (2.32, true, "Jupiter", 4);
insert into planets (name) values ("Neptune");
insert into planets (moons, gravity) values (4, 2.42);
drop table planets;

close sol;
drop database sol;
exit;
