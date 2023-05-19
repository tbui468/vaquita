if exists drop database sol;
create database sol;
open sol;

create table planets (name string, gravity float, mass int, atmosphere bool);
insert into planets (name, gravity, mass, atmosphere) values ("Mars", -2.32, 99, true);
select * from planets;
close sol;
exit;
