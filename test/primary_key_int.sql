create database sol;
open sol;

create table planets (id int key, name string, mass int, atmosphere bool);
insert into planets (id, name, mass, atmosphere) values 
    (3, "Mars", 10, true), 
    (-2, "Venus", 10, false),
    (1, "Neptune", 20, false);
select * from planets;
drop table planets;

close sol;
drop database sol;
exit;
