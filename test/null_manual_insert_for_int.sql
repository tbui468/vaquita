create database sol;
open sol;

create table planets (name string, moons int);
insert into planets (name, moons) values ("Mars", null);
select * from planets;

close sol;
drop database sol;
exit;
