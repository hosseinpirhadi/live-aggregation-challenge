-- CREATE DATABASE data_engineer;

-- \c data_engineer

CREATE SCHEMA bank;

CREATE TABLE bank.holding (
    price int,
    volume int,
    datetime_created timestamp DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(datetime_created)
);

ALTER TABLE bank.holding REPLICA IDENTITY FULL;

insert into bank.holding values (1000, 100, now());
insert into bank.holding values (1005, 550, now());
insert into bank.holding values (1015, 600, now());
insert into bank.holding values (1025, 650, now());