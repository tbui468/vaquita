if exists drop database sol;
create database sol;
open sol;

create table planets (name string, mass int, atmosphere bool);
describe planets;
insert into planets (name, mass, atmosphere) values ("Neptune", 20, false), ("Mars", 10, true), ("Venus", 10, false), ("Earth", 10, true);
insert into planets (name, mass, atmosphere) values ("Mercury", 5, true), ("Jupiter", 100, true), ("Saturn", 323, false);
select mass, name from planets;

create table asteroids (name string, mass int, material string);
insert into asteroids (name, mass, material) values ("d242", 100, "ice"), ("c242", 99, "rock");
insert into asteroids (material) values ("iron"), ("nickel");
select id, name, material from asteroids;
select name, id from planets;
describe asteroids;
select name from planets where not mass = 10 and name = "Jupiter";

close sol;
exit;
