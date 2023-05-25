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
        ("Venus", 4.87e2, 0, 92.0, false),
        ("Earth", -5.97e-2, 1, 1.0, false),
        ("Mars", 0.23e-2, 2, 0.01, false);

select * from planets;
close sol;
drop database sol;
exit;
