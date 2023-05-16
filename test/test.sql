if exists drop database sol;
create database sol;
open sol;
create table planets (name string, mass int, atmosphere bool);
describe planets;
insert into planets (name, mass, atmosphere) values ("Neptune", 20, false), ("Mars", 10, true);
select * from planets;
close sol;
exit;
