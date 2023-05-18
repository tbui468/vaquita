# VaquitaSQL - an SQL-92 Implementation

## Basic Usage
### Building
git clone https://github.com/tbui468/vaquita.git\
cd vaquita\
mkdir build\
cd build\
cmake ..\
cmake --build .\

### CLI
\> create database pets;\
\> open pets;\
\> create table cats (name string, age, int, loud bool);\
\> insert into cats (name, age, loud) values ("Mittens", 2, true), ("Saki", 3, false);\
\> select name, age from cats where id = 1;\
\> close pets;\
\> exit;\

## ACID Compliance
### Progress
### Atomicity
### Consistency
### Isolation
### Durability

## SQL-92 Conformance
### Entry
#### Statements
select\
from\
where\
group by\
having\
insert\
update\
delete\
create table\
create view\

#### Constraints/Column Types
unique\
not null\
default\
check\
primary keys\
foreign keys\

#### Data Types
numeric\
decimal\
integer\
smallint\
float\
real\
double\

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
