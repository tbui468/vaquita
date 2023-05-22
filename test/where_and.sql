create database sol;
open sol;

create table planets (name string, moons int);
insert into planets (name, moons) values ("Mars", 2);
select * from planets where true and true;
select * from planets where true and false;
select * from planets where false and false;
select * from planets where false and true;

close sol;
drop database sol;
exit;
