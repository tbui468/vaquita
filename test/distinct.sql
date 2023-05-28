if exists drop database sol;
create database sol;
open sol;

create table planets (
                        name string, 
                        mass float, 
                        number_of_moons int,
                        surface_pressure float,
                        ring_system bool
                    );

insert into planets (
                        name, 
                        mass, 
                        number_of_moons, 
                        surface_pressure,
                        ring_system
                    ) 

values
        ("Venus", 2.32, 0, 92.0, true),
        ("Venus", null, 1, 1.0, false),
        ("Mars", 4.23, 2, 0.01, true),
        ("Mars", 2.23, 2, 0.01, true);

select distinct name, ring_system from planets;
close sol;
drop database sol;
exit;
