create database sol;
open sol;

create table planets (id int8 key, name text, moons int8);

insert into planets (id, name) values (1, "Mars");
insert into planets (id, name, moons) values (2, "Earth", 1);
select * from planets where moons is null;

close sol;
drop database sol;
exit;
