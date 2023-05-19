if exists drop database sol;
create database sol;
open sol;

create table planets (name string, gravity float, mass int, atmosphere bool);

close sol;
exit;
