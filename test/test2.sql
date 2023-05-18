if exists drop database sol;
create database sol;
open sol;

create table planets (name string, gravity float, mass int, atmosphere bool);
describe planets;
insert into planets (name, gravity, mass, atmosphere) values ("Mars", 1.232, 2324, true), ("Earth", 1.0, 232, true);
insert into planets (name, gravity) values ("Saturn", 22.42), ("Jupiter", 24.232);
select * from planets;

drop table planets;

close sol;
exit;
