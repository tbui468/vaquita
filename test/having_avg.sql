if exists drop database sol;
create database sol;
open sol;

create table planets (
                        id int key,
                        name string, 
                        mass float, 
                        moons int,
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
        (1, "Venus", 5.32, 0, false),
        (2, "Earth", 0.0, 1, true),
        (3, "Mars", 4.23, 2, true),
        (4, "Saturn", 4.23, 2, true);

select ring_system, avg(mass) from planets group by ring_system having avg(mass) > 5.0;
close sol;
drop database sol;
exit;
