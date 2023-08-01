create database sol;
open sol;

create table planets (id int8 key, name text, moons int8);
insert into planets (id, name, moons) values (1, "Mars", 2);
select * from planets where true and true;
select * from planets where true and false;
select * from planets where false and false;
select * from planets where false and true;

close sol;
drop database sol;
exit;
