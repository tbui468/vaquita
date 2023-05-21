create database sol;
open sol;

create table planets (name string, moons int, gravity float);

insert into planets (name, gravity) values ("Mars", 2.42), ("Earth", 24.23);
select * from planets;

close sol;
drop database sol;
exit;
