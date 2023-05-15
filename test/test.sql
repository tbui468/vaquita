create database sol;
open sol;
create table planets (name string);
insert into planets (name) values ("Mars");
select * from planets;
close sol;
exit;
