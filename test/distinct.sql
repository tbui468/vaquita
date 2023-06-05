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
        (1, "Venus", 2.32, 0, 92.0, true),
        (2, "Venus", null, 1, 1.0, false),
        (3, "Mars", 2.32, 5, 934.3, true),
        (4, "Mars", 4.23, 2, 0.01, true);

select distinct name, ring_system from planets;
close sol;
drop database sol;
exit;
