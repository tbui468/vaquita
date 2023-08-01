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
        (2, "Earth", 5.0, 1, true),
        (3, "Mars", 4.23, 2, true),
        (4, "Saturn", 3.23, 2, true);

select ring_system, min(mass) from planets group by ring_system having min(mass) < 3.0;
select ring_system, min(mass) from planets group by ring_system having min(mass) > 3.0;
close sol;
drop database sol;
exit;
