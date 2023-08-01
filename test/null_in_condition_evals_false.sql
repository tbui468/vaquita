if exists drop database sol;
create database sol;
open sol;

create table planets (
                        id int8 key,
                        name text, 
                        mass float8, 
                        moons int8,
                        ring_system bool
                    );

insert into planets (
                        id,
                        name, 
                        mass, 
                        moons, 
                        ring_system
                    ) 

values
        (1, "Venus", 10.0, 1, true),
        (2, "Earth", null, null, null),
        (3, "Mars", 0.0, 2, false);

select name from planets where mass > 5.0;
select name from planets where moons = 1;
select name from planets where ring_system = true;
select name from planets where mass < 5.0;
select name from planets where moons <> 1;
select name from planets where ring_system = false;
close sol;
drop database sol;
exit;
