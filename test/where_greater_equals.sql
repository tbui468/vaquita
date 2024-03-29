create database sol;
open sol;

create table planets (id int8 key, name text, moons int8, gravity float8);
insert into planets (id, name, moons, gravity) values (1, "Mars", 2, 7.89), (2, "Earth", 1, 9.81);
select * from planets where name >= "Earth";
select * from planets where moons >= 1;
select * from planets where gravity >= 8.00;

close sol;
drop database sol;
exit;
