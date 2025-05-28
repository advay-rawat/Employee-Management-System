CREATE TABLE employee_strikes (
    employee_id TEXT primary key,
    salary DOUBLE PRECISION,
    strike_1 real DEFAULT Null,
    strike_2 real DEFAULT Null,
    strike_3 real DEFAULT Null,
    strike_4 real DEFAULT Null,
    strike_5 real DEFAULT Null,
    strike_6 real DEFAULT Null,
    strike_7 real DEFAULT Null,
    strike_8 real DEFAULT Null,
    strike_9 real DEFAULT Null,
    strike_10 real DEFAULT Null,
    no_of_strikes INTEGER default 0
);

create table flagged_messages(
	employee_id TEXT,
	start_date TIMESTAMP
);

CREATE TABLE employee_strikes_stg (
    employee_id TEXT primary key,
    salary DOUBLE PRECISION,
    strike_1 real DEFAULT Null,
    strike_2 real DEFAULT Null,
    strike_3 real DEFAULT Null,
    strike_4 real DEFAULT Null,
    strike_5 real DEFAULT Null,
    strike_6 real DEFAULT Null,
    strike_7 real DEFAULT Null,
    strike_8 real DEFAULT Null,
    strike_9 real DEFAULT Null,
    strike_10 real DEFAULT Null,
    no_of_strikes INTEGER default 0
);

