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
        (1, "Venus", 2.32, 0, false),
        (2, "Earth", null, 1, true),
        (3, "Mars", 4.23, 2, true),
        (4, "Saturn", 4.23, 2, true);

select ring_system, sum(moons) from planets group by ring_system having sum(moons) > 3;
close sol;
drop database sol;
exit;
