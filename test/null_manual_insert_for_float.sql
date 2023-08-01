create database sol;
open sol;

create table planets (id int8 key, name text, gravity float8);
insert into planets (id, name, gravity) values (1, "Mars", null);
select * from planets;

close sol;
drop database sol;
exit;
