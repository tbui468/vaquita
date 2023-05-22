create database sol;
open sol;

create table planets (name string, moons int);

insert into planets (name) values ("Mars");
insert into planets (name, moons) values ("Earth", 1);
select * from planets where moons is null;

close sol;
drop database sol;
exit;
