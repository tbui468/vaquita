create database sol;
open sol;

create table planets (name string, moons int, rings bool);
insert into planets (name, moons, rings) values ("Mars", 2, true), ("Earth", 1, false);
select * from planets where name <> "Earth";
select * from planets where moons <> 1;
select * from planets where rings <> false;

close sol;
drop database sol;
exit;
