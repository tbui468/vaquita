if exists drop database sol;
create database sol;
open sol;

create table planets (
                        id int8 key,
                        name text, 
                        mass float8, 
                        number_of_moons int8,
                        surface_pressure float8,
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
        (1, "Venus", 4.87e2, 0, 92.0, false),
        (2, "Earth", -5.97e-2, 1, 1.0, false),
        (3, "Mars", 0.23e-2, 2, 0.01, false);

select * from planets;
close sol;
drop database sol;
exit;
