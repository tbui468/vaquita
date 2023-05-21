create database sol;
open sol;

create table planets (name string, rings bool);
insert into planets (name, gravity) values ("Mars", null);
select * from planets;

close sol;
drop database sol;
exit;
