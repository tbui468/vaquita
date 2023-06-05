create database sol;
open sol;

create table planets (id int key, name string, moons int, gravity float);

insert into planets (id, name, gravity) values (1, "Mars", 2.42), (2, "Earth", 24.23);
select * from planets;

close sol;
drop database sol;
exit;
