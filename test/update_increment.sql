create database sol;
open sol;

create table planets (id int8 key, name text, moons int8);
insert into planets (id, name, moons) values (1, "Neptune", 20), (2, "Mars", 10), (3, "Venus", 10);
update planets set moons = moons + 100;
select * from planets;
drop table planets;

close sol;
drop database sol;
exit;
