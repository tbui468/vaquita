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
        ("Mercury", 0.330, 0, 0.0, false),
        ("Venus", 4.87, 0, 92.0, false),
        ("Earth", 5.97, 1, 1.0, false),
        ("Mars", 0.642, 2, 0.01, false),
        ("Jupiter", 1898.0, 92, null, true),
        ("Saturn", 568.0, 83, null, true),
        ("Uranus", 86.8, 27, null, true),
        ("Neptune", 102.0, 14, null, true);

select * from planets where null is null;
describe planets;
select name from planets where null is not null;

close sol;
drop database sol;
exit;
