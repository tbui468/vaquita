create database sol;
open sol;

create table planets (name string, moons int, rings bool);
insert into planets (name, moons, rings) values ("Mars", 2, true), ("Earth", 1, false);

close sol;
drop database sol;
exit;
