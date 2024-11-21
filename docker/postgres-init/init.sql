-- Create a new logical database
create database sequin_playground;

-- Connect to the new database
\c sequin_playground
-- Create tables and seed data
create table products(
  id serial primary key,
  name varchar(100),
  price decimal(10, 2),
  inserted_at timestamp default now(),
  updated_at timestamp default now()
);

alter table products replica identity
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
  before update on products for each row
  execute procedure update_timestamp();

insert into products(name, price)
  values ('Avocados (3 pack)', 5.99),
('Flank Steak (1 lb)', 8.99),
('Salmon Fillet (12 oz)', 14.99),
('Baby Spinach (16 oz)', 4.99),
('Sourdough Bread', 6.99),
('Blueberries (6 oz)', 3.99);

create publication sequin_pub for all tables;

select
  pg_create_logical_replication_slot('sequin_slot', 'pgoutput');

