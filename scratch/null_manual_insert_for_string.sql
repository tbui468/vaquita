create database sol;
open sol;

create table planets (name string, system string);
insert into planets (name, system) values ("Mars", null);
select * from planets;

close sol;
drop database sol;
exit;
