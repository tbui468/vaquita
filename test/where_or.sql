create database sol;
open sol;

create table planets (name string, moons int);
insert into planets (name, moons) values ("Mars", 2);
select * from planets where true or true;
select * from planets where true or false;
select * from planets where false or false;
select * from planets where false or true;

close sol;
drop database sol;
exit;
