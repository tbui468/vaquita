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
        ("Venus", 2.32, 0, 92.0, false),
        ("Earth", null, 1, 1.0, true),
        ("Mars", 4.23, 2, 0.01, false),
        ("Saturn", 4.23, 2, 0.01, false);

select ring_system, sum(number_of_moons) from planets group by ring_system;
select ring_system, sum(10) from planets group by ring_system;
close sol;
drop database sol;
exit;
