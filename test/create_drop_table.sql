create database sol;
open sol;

create table planets (id int8 key, name text, moons int8, gravity float8, atmosphere bool);
drop table planets;

close sol;
drop database sol;
exit;
