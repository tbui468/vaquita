create database sol;
open sol;

create table planets (id int8 key, name text, rings bool);
insert into planets (id, name, rings) values (1, null, false);
select * from planets;

close sol;
drop database sol;
exit;
