create database sol;
open sol;

create table planets (id int8 key, name text, moons int8);
insert into planets (id, name, moons) values (1, "Mars", 2);
select * from planets where true or true;
select * from planets where true or false;
select * from planets where false or false;
select * from planets where false or true;

close sol;
drop database sol;
exit;
