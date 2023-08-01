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

select * from planets;

close sol;
drop database sol;
exit;
