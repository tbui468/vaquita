create database sol;
open sol;

create table planets (name string, mass int, atmosphere bool);
insert into planets (name, mass, atmosphere) values ("Neptune", 20, false), ("Mars", 10, true), ("Venus", 10, false), ("Earth", 10, true);
insert into planets (name, mass, atmosphere) values ("Mercury", 5, true), ("Jupiter", 100, true), ("Saturn", 323, false);
select name from planets;
select mass from planets;
select atmosphere from planets;
select name, atmosphere from planets;
select mass, atmosphere, name from planets;
select atmosphere, name, mass from planets;
drop table planets;

close sol;
drop database sol;
exit;