create database sol;
open sol;

create table planets (id int key, name string, moons int, gravity float, atmosphere bool);
drop table planets;

close sol;
drop database sol;
exit;
