-- Create a new logical database
create database sequin_playground;

-- Connect to the new database
\c sequin_playground
-- Create tables and seed data
create table regions(
  id serial primary key,
  name varchar(100),
  timezone text,
  inserted_at timestamp default now(),
  updated_at timestamp default now()
);

create or replace function public.update_timestamp()
  returns trigger
  language plpgsql
  as $function$
begin
  if new.updated_at = old.updated_at then
    new.updated_at = NOW();
  end if;
  return NEW;
end;
$function$;

create trigger update_timestamp
  before update on regions for each row
  execute procedure update_timestamp();

insert into
  regions (name, timezone)
values
  ('us-east-1', 'est'),
  ('us-west-1', 'pst'),
  ('us-west-2', 'pst'),
  ('us-east-2', 'est'),
  ('us-central-1', 'cst');

create publication sequin_pub for all tables;

select
  pg_create_logical_replication_slot('sequin_slot', 'pgoutput');

