create database sol;
open sol;

create table planets (id int8 key, name text, moons int8);
insert into planets (id, name) values (1, "Earth");
insert into planets (id, name, moons) values (2, "Mars", 2);
select * from planets where moons is not null;

close sol;
drop database sol;
exit;
