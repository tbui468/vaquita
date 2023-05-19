if exists drop database sol;
create database sol;
open sol;

create table planets (
                        name string, 
                        mass float, 
                        diameter int, 
                        density int, 
                        gravity float, 
                        escape_velocity float, 
                        rotation_period float, 
                        length_of_day float, 
                        distance_from_sun float, 
                        perihelion float, 
                        aphelion float, 
                        orbital_period float, 
                        oribital_velocity float, 
                        orbital_inclination float, 
                        orbital_eccentricity float, 
                        obliquity_to_orbit float, 
                        mean_temperature int, 
                        surface_pressure float, 
                        number_of_moons int, 
                        ring_system bool, 
                        global_magnetic_field bool
                    );

insert into planets (
                        name, 
                        mass, 
                        diameter, 
                        density, 
                        gravity, 
                        escape_velocity, 
                        rotation_period, 
                        length_of_day, 
                        distance_from_sun, 
                        perihelion, 
                        aphelion, 
                        orbital_period, 
                        oribital_velocity, 
                        orbital_inclination, 
                        orbital_eccentricity, 
                        obliquity_to_orbit, 
                        mean_temperature, 
                        surface_pressure, 
                        number_of_moons, 
                        ring_system, 
                        global_magnetic_field
                    ) 

values
        ("Mercury", 0.330, 4879, 5429, 3.7, 4.3, 1407.6, 4222.6, 57.9, 46.0, 69.8, 88.0, 47.4, 7.0, 0.206, 0.034, 167, 0, 0, false, true),
        ("Venus", 4.87, 12104, 5243, 8.9, 10.4, -5832.5, 2802.0, 108.2, 107.5, 108.9, 224.7, 35.0, 3.4, 0.007, 177.4, 464, 92, 0, false, false);

select * from planets;

close sol;
exit;
