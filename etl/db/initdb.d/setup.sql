-- TODO: secrets
-- TODO: Guards

create role cache_read_write valid until 'infinity';
create database cache_db;
grant all privileges on database cache_db to cache_read_write;

create user cache_user with encrypted password 'cache_pwd' valid until 'infinity';
grant cache_read_write to cache_user;

\connect cache_db;

set role cache_user;

create schema if not exists cache;

create table if not exists cache.machine_data_file
(
    created_at timestamp not null default now(),
    id         uuid      primary key default gen_random_uuid(),

    has_error         boolean   not null,
    run_error_count   smallint  not null,
    run_success_count smallint  not null,
    source            varchar   not null unique -- TODO: Something better than the filename
);

create table if not exists cache.machine_data_run
(
    created_at timestamp not null default now(),
    id         uuid      primary key, -- No default, using UUID from data

    available_channels varchar[] not null,
    dataframe_s3_uri   varchar   not null,
    has_error          boolean   not null,
    machine_file_id    uuid      not null,
    start_time         timestamp not null,
    stop_time          timestamp not null,
    run_error_count    smallint  not null,
    run_success_count  smallint  not null,
    constraint fk_machine_file_id foreign key(machine_file_id) references cache.machine_data_file(id)
);

create table if not exists cache.machine_data_run_robot
(
    created_at timestamp not null default now(),
    id         uuid      primary key default gen_random_uuid(),

    machine_run_id    uuid      not null,
    robot_id          smallint  not null,
    total_distance_mm float     not null,
    constraint fk_machine_run_id foreign key(machine_run_id) references cache.machine_data_run(id)
);
