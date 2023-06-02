if exists drop database sol;
create database sol;
open sol;

create table planets (
                        name string, 
                        mass float, 
                        moons int,
                        ring_system bool
                    );

insert into planets (
                        name, 
                        mass, 
                        moons,
                        ring_system
                    ) 

values
        ("Venus", 5.32, 0, false),
        ("Earth", 0.0, 1, true),
        ("Mars", 4.23, 2, true),
        ("Saturn", 4.23, 2, true);

select ring_system, count(1) from planets group by ring_system having count(1) < 2;
close sol;
drop database sol;
exit;
