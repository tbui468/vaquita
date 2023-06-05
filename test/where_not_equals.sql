create database sol;
open sol;

create table planets (id int key, name string, moons int, rings bool);
insert into planets (id, name, moons, rings) values (1, "Mars", 2, true), (2, "Earth", 1, false);
select * from planets where name <> "Earth";
select * from planets where moons <> 1;
select * from planets where rings <> false;

close sol;
drop database sol;
exit;
