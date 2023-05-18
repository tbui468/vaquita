if exists drop database sol;
create database sol;
open sol;

create table planets (name string, mass int, atmosphere bool);
insert into planets (name, mass, atmosphere) values ("Neptune", 20, false), ("Mars", 10, true), ("Venus", 10, false), ("Earth", 10, true);
insert into planets (name, mass, atmosphere) values ("Mercury", 5, true), ("Jupiter", 100, true), ("Saturn", 323, false);
select id, name from planets where mass < 10 or mass > 200;

close sol;
exit;
