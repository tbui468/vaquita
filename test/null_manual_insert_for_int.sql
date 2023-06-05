create database sol;
open sol;

create table planets (id int key, name string, moons int);
insert into planets (id, name, moons) values (1, "Mars", null);
select * from planets;

close sol;
drop database sol;
exit;
