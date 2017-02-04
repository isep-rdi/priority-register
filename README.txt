

This is prrof-of-the concept implementation of the Priority-Register idea described in this paper.

To test it:

create keyspace test with replication = {'class' : 'SimpleStrategy', 'replication_factor' : 3};

create table sensor_reading(city text, date int, time int, precision float, sen_rank float, temperature int, primary key((city, date), time, precision, sen_rank)) with replacement_ordering=1 AND replacement_priority=3;

insert into sensor_reading(city, date, time, precision, sen_rank, temperature) values ('Loc1', 110515, 1050, 8.5, 5.2, 42);
insert into sensor_reading(city, date, time, precision, sen_rank, temperature) values ('Loc1', 110515, 1050, 8.5, 5.3, 43);
insert into sensor_reading(city, date, time, precision, sen_rank, temperature) values ('Loc1', 110515, 1051, 8.6, 5.2, 46);
insert into sensor_reading(city, date, time, precision, sen_rank, temperature) values ('Loc1', 110515, 1051, 8.5, 5.2, 55);
insert into sensor_reading(city, date, time, precision, sen_rank, temperature) values ('Loc1', 110515, 1050, 8.6, 5.2, 34);

select * from sensor_reading;

Experiments with additional non-clustering column:

create table sensor_reading(loc_id text, date int, time int, precision float, sen_rank float, temperature int, humidity int, primary key(loc_id, date, time, precision, sen_rank)) with replacement_ordering=2 AND replacement_priority=3;

insert into sensor_reading(loc_id, date, time, precision, sen_rank, temperature, humidity) values ('Loc1', 110515, 1050, 8.5, 5.2, 32, 59);
insert into sensor_reading(loc_id, date, time, precision, sen_rank, temperature, humidity) values ('Loc1', 110515, 1050, 8.5, 5.3, 33, 60);
insert into sensor_reading(loc_id, date, time, precision, sen_rank, temperature, humidity) values ('Loc1', 110515, 1051, 8.6, 5.2, 36, 61);
insert into sensor_reading(loc_id, date, time, precision, sen_rank, temperature, humidity) values ('Loc1', 110515, 1051, 8.5, 5.2, 25, 70);
insert into sensor_reading(loc_id, date, time, precision, sen_rank, temperature, humidity) values ('Loc1', 110515, 1050, 8.6, 5.2, 14, 85);

select * from sensor_reading;

Also try with follwoing queries:

insert into sensor_reading(loc_id, date, time, precision, sen_rank, humidity) values ('Loc1', 110515, 1051, 8.6, 5.1, 59);

insert into sensor_reading(loc_id, date, time, precision, sen_rank, humidity) values ('Loc1', 110515, 1051, 8.6, 5.2, 63);

insert into sensor_reading(loc_id, date, time, precision, sen_rank, humidity) values ('Loc1', 110515, 1051, 8.6, 5.3, 64);

insert into sensor_reading(loc_id, date, time, precision, sen_rank, temperature) values ('Loc1', 110515, 1051, 8.6, 5.3, 34);
