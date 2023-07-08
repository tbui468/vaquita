# VaquitaSQL - an SQL-92 Implementation

## Basic Usage
### Clone the Repository
git clone https://github.com/tbui468/vaquita.git<br>

### Building the Server
cd vaquita<br>
cd server<br>
mkdir build<br>
cd build<br>
cmake ..<br>
cmake --build .<br>

### Building the Client
cd vaquita<br>
cd client<br>
mkdir build<br>
cd build<br>
cmake ..<br>
cmake --build .<br>

### Start the Server
./vdb

### Client Using REPL
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

### Client Using Script
./vdbclient script.sql<br>

## Currently Supported SQL
### Data Definition
create database <database>;
show databases;
drop database <database>;
if exists drop database <database>;
open <database>;

show tables;
describe <table>;
drop table <table>;
if exists drop table <table>;
create table <table> (<column definitions>);

### Data Manipulation
insert into <table> (<field>) values (<values>);

### Data Query
select distinct <projection> from <table> where <selection> order by <field> desc limit <count>;
select <projection/aggregate> from <table> group by <field> having <aggregate selection>;

### Data Control
NA

## Todo
Update
Delete
Joins
Foreign Keys
Transactions
Subqueries
Derived Tables
Indexing
Logging
Page Eviction
Locking
Better Error Handling
