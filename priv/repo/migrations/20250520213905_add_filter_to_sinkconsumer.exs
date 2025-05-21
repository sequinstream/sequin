defmodule Sequin.Repo.Migrations.AddFilterToSinkconsumer do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    execute """
            alter table #{@config_schema}.sink_consumers add column filter_id uuid references #{@config_schema}.functions(id) on delete restrict
            """,
            """
            alter table #{@config_schema}.sink_consumers drop column filter_id
            """

    execute """
            create or replace function #{@config_schema}.check_filter_id_type()
            returns trigger as $$
            begin
              if new.filter_id is not null then
                if not exists (
                  select 1 from #{@config_schema}.functions 
                  where id = new.filter_id and type = 'filter'
                ) then
                  raise exception 'filter_id must reference a function with type ''filter''';
                end if;
              end if;
              return new;
            end;
            $$ language plpgsql;
            """,
            """
            drop function if exists #{@config_schema}.check_filter_id_type() cascade;
            """

    execute """
            create trigger enforce_filter_id_function_type
            before insert or update on #{@config_schema}.sink_consumers
            for each row execute function #{@config_schema}.check_filter_id_type();
            """,
            """
            drop trigger if exists enforce_filter_id_function_type on #{@config_schema}.sink_consumers;
            """
  end
end
