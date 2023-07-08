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

select * from planets;

close sol;
drop database sol;
exit;
