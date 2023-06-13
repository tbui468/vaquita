create database sol;
open sol;

create table planets (id bool key, name string, mass int, atmosphere bool);
insert into planets (id, name, mass, atmosphere) values 
    (true, "Mars", 10, true), 
    (false, "Venus", 10, false);
select * from planets;
drop table planets;

close sol;
drop database sol;
exit;
