-- Create a new logical database
create database sequin_playground;

-- Connect to the new database
\c sequin_playground
-- Create tables and seed data
create table groceries(
  id serial primary key,
  name varchar(100),
  section varchar(100),
  checked boolean default false,
  inserted_at timestamp default now(),
  updated_at timestamp default now()
);

alter table groceries replica identity
  full;

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
  before update on groceries for each row
  execute procedure update_timestamp();

insert into groceries(name, section)
  values ('Apple', 'produce'),
  ('Banana', 'produce'),
  ('Bread', 'bakery'),
  ('Milk', 'dairy');

create publication sequin_pub for all tables;

select
  pg_create_logical_replication_slot('sequin_slot', 'pgoutput');

