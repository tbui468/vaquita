# VaquitaSQL - an SQL Implementation

## Basic Usage
### Building the Server

```
cd vaquita
cd server
mkdir build
cd build
cmake ..
cmake --build .
```

### Building the Client

```
cd vaquita
cd client
mkdir build
cd build
cmake ..
cmake --build .
```

### Client Using REPL

```
./vdbclient

vdb> create database sol;
created database sol

vdb> open sol;
opened database sol

vdb> create table planets (id int key, name string, mass float, rings bool);
created table planets

vdb> insert into planets (id, name, mass, rings) values (1, "Earth", 1.00, false), (2, "Mars", 0.5, false), (3, "Saturn", 3.0, true);
inserted 3 record(s) into planets

vdb> select * from planets;
+----+--------+----------+-------+
| id | name   | mass     | rings |
+----+--------+----------+-------+
| 1  | Earth  | 1.000000 | false |
| 2  | Mars   | 0.500000 | false |
| 3  | Saturn | 3.000000 | true  |
+----+--------+----------+-------+

vdb> select id, name from planets where mass > 0.8;
+----+--------+
| id | name   |
+----+--------+
| 1  | Earth  |
| 3  | Saturn |
+----+--------+

vdb> select avg(mass) from planets group by rings;
+-----------+
| avg(mass) |
+-----------+
| 0.750000  |
| 3.000000  |
+-----------+

vdb> close sol;
closed database sol

vdb> exit;
```

### Client Using Script

```
./vdbclient script.sql
```

## Supported SQL

### Data Types
int<br>
float<br>
string<br>
bool<br>

### Data Definition and Manipulation
create database [name]<br>
drop database [name]<br>
if exsits drop database [name]<br>
show databases<br>
open [name]<br>
close [name]<br>

create table [name] [column definitions]<br>
drop table [name]<br>
if exsits drop table [name]<br>
show tables<br>
describe [name]<br>

insert into [table] [columns] values [values]<br>
update [table] set [columns = values]<br>
delete from [table]<br>

### Data Query<br>
select<br>
distinct<br>
where<br>
order by<br>
desc<br>
limit<br>
group by<br>
having<br>
avg<br>
count<br>
max<br>
min<br>
sum<br>

## Architecture

Image goes here<br>

## Not Yet Implemented
Joins<br>
Alter<br>
Foreign Keys<br>
Transactions<br>
Subqueries<br>
Derived Tables<br>
Indexing<br>
Logging<br>
Page Eviction<br>
Locking<br>
Improved Error Handling/Messages<br>
