create schema if not exists `redmate`;

create table if not exists `redmate`.`departments` (
       id int primary key,
       name varchar(255) not null
);

create table if not exists `redmate`.`employees` (
       id int primary key,
       name varchar(255) not null,
       salary decimal(19,2) not null,
       department_id int not null references `redmate`.`departments`(id)
);

delete from `redmate`.`departments`;

delete from `redmate`.`employees`;

insert into `redmate`.`departments` (id, name)
values
(1, 'IT'), (2, 'Marketing'), (3, 'Sales');

insert into `redmate`.`employees` (id, name, salary, department_id)
values
(1, 'Mark', 100000, 1),
(2, 'Sergey', 120000, 1),
(3, 'Sally', 80000, 2),
(4, 'Stuart', 75000, 2),
(5, 'Martin', 50000, 3);
