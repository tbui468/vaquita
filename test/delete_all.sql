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
        (1, "Venus", 4.87, 0, 92.0, false),
        (2, "Earth", 5.97, 1, 1.0, false),
        (3, "Mars", 0.642, 2, 0.01, false);

delete from planets;
select * from planets;

close sol;
drop database sol;
exit;
