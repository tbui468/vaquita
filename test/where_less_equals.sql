create database sol;
open sol;

create table planets (name string, moons int, gravity float);
insert into planets (name, moons, gravity) values ("Mars", 2, 7.89), ("Earth", 1, 9.81);
select * from planets where name <= "Earth";
select * from planets where moons <= 1;
select * from planets where gravity <= 8.00;

close sol;
drop database sol;
exit;
