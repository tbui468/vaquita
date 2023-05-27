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
        ("Earth", null, 1, 1.0, false),
        ("Mars", 4.23, 2, 0.01, false);

select * from planets order by name;
select * from planets where number_of_moons >= 1 order by number_of_moons desc;
select * from planets where number_of_moons >= 1 order by number_of_moons;
close sol;
drop database sol;
exit;
