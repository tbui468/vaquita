create database sol;
open sol;

create table planets (name string, mass int, atmosphere bool);
insert into planets (name, mass, atmosphere) values ("Neptune", 20, false), ("Mars", 10, true), ("Venus", 10, false), ("Earth", 10, true);
insert into planets (name, mass, atmosphere) values ("Mercury", 5, true), ("Jupiter", 100, true), ("Saturn", 323, false);
select * from planets limit 5;
select * from planets order by id desc limit 5;
drop table planets;

close sol;
drop database sol;
exit;
