create database sol;
open sol;

create table planets (name string, moons int);
insert into planets (name) values ("Earth");
insert into planets (name, moons) values ("Mars", 2);
select * from planets where moons is not null;

close sol;
drop database sol;
exit;
