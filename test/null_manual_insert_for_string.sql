create database sol;
open sol;

create table planets (name string, rings bool);
insert into planets (name, rings) values (null, false);
select * from planets;

close sol;
drop database sol;
exit;
