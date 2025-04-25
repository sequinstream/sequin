defmodule Sequin.Repo.Migrations.AddRoutingToSinkconsumer do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    execute """
            alter table #{@config_schema}.sink_consumers add column routing_id uuid references #{@config_schema}.transforms(id) on delete restrict
            """,
            """
            alter table #{@config_schema}.sink_consumers drop column routing_id
            """

    execute """
            create or replace function #{@config_schema}.check_routing_id_type()
            returns trigger as $$
            begin
              if new.routing_id is not null then
                if not exists (
                  select 1 from #{@config_schema}.transforms 
                  where id = new.routing_id and type = 'routing'
                ) then
                  raise exception 'routing_id must reference a transform with type ''routing''';
                end if;
              end if;
              return new;
            end;
            $$ language plpgsql;
            """,
            """
            drop function if exists #{@config_schema}.check_routing_id_type() cascade;
            """

    execute """
            create trigger enforce_routing_id_transform_type
            before insert or update on #{@config_schema}.sink_consumers
            for each row execute function #{@config_schema}.check_routing_id_type();
            """,
            """
            drop trigger if exists enforce_routing_id_transform_type on #{@config_schema}.sink_consumers;
            """
  end
end
