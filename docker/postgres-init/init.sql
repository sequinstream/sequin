-- Create a new logical database
create database sequin_playground;

-- Connect to the new database
\c sequin_playground
-- Create tables and seed data
create table users(
  id serial primary key,
  name varchar(100),
  house_name text,
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
  before update on users for each row
  execute procedure update_timestamp();

insert into users(name, house_name)
  values ('Paul Atreides', 'House Atreides'),
('Lady Jessica', 'Bene Gesserit'),
('Duncan Idaho', 'House Atreides'),
('Chani', 'Fremen');

create publication sequin_pub for all tables;

select
  pg_create_logical_replication_slot('sequin_slot', 'pgoutput');

