if exists drop database sol;
create database sol;
open sol;

create table planets (
                        id int key,
                        name string, 
                        mass float, 
                        number_of_moons int,
                        surface_pressure float,
                        ring_system bool
                    );

insert into planets (
                        id,
                        name, 
                        mass, 
                        number_of_moons, 
                        surface_pressure,
                        ring_system
                    ) 

values
        (1, "Venus", 2.32, 0, 92.0, false),
        (2, "Earth", null, 1, 1.0, true),
        (3, "Mars", 4.23, 2, 0.01, false),
        (4, "Saturn", 4.23, 2, 0.01, false);

select ring_system, sum(number_of_moons) from planets group by ring_system;
select ring_system, sum(10) from planets group by ring_system;
close sol;
drop database sol;
exit;
