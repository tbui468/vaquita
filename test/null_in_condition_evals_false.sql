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
        ("Venus", 10.0, 1, true),
        ("Earth", null, null, null),
        ("Mars", 0.0, 2, false);

select name from planets where mass > 5.0;
select name from planets where moons = 1;
select name from planets where ring_system = true;
select name from planets where mass < 5.0;
select name from planets where moons <> 1;
select name from planets where ring_system = false;
close sol;
drop database sol;
exit;
