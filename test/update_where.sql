create database sol;
open sol;

create table planets (id int key, name string, mass float, atmosphere bool, moons int);
insert into planets (id, name, mass, atmosphere, moons) values (1, "Venus", 20.0, false, 0), (2, "Earth", 10.0, true, 1), (3, "Mars", 10.0, false, 2);
update planets set name="new_name", mass=0.0, atmosphere=true, moons=0 where id = 2;
select * from planets;

close sol;
drop database sol;
exit;
