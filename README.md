# VaquitaSQL - an SQL-92 Implementation

## Basic Usage
### Building
git clone https://github.com/tbui468/vaquita.git<br>
cd vaquita<br>
mkdir build<br>
cd build<br>
cmake ..<br>
cmake --build .<br>

###REPL
./vdb
\> create database pets;<br>
\> open pets;<br>
\> create table cats (name string, age, int, loud bool);<br>
\> insert into cats (name, age, loud) values ("Mittens", 2, true), ("Saki", 3, false);<br>
\> select name, age from cats where id = 1;<br>
\> close pets;<br>
\> exit;<br>

###Running a Script
./vdb myscript.sql<br>

## ACID Compliance
### Atomicity (TODO)
### Consistency (TODO)
### Isolation (TODO)
### Durability (TODO)

## SQL-92 Conformance
### Entry
#### Statements
select<br>
from<br>
where<br>
group by<br>
having<br>
insert<br>
update<br>
delete<br>
create table<br>
create view<br>

#### Constraints/Column Types
unique<br>
not null<br>
default<br>
check<br>
primary keys<br>
foreign keys<br>

#### Data Types
numeric<br>
decimal<br>
integer<br>
smallint<br>
float<br>
real<br>
double<br>

### Intermediate
#### TODO: fill in requirements
### Full
#### TODO: fill in requirements

## Architecture

### Client
#### CLI
#### Parser
#### Code Generator (TODO)

### Server
#### Virtual Machine (TODO)
#### B-Tree
#### Logger (TODO)
#### Pager
##### pager should manage locking (TODO)
