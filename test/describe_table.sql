create database sol;
open sol;

create table planets (id int key, name string, moons int, gravity float, atmosphere bool);
describe planets;
drop table planets;

close sol;
drop database sol;
exit;
