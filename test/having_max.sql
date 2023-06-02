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
        ("Venus", 2.32, 0, false),
        ("Earth", 5.0, 1, true),
        ("Mars", 4.23, 2, true),
        ("Saturn", 3.23, 2, true);

select ring_system, max(mass) from planets group by ring_system having max(mass) > 0.0 and max(mass) < 4.0;
select ring_system, max(mass) from planets group by ring_system having max(mass) > 0.0 and max(mass) < 6.0;
close sol;
drop database sol;
exit;
