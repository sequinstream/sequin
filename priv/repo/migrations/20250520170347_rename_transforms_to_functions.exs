defmodule Sequin.Repo.Migrations.RenameTransformsToFunctions do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    rename table(:transforms, prefix: @config_schema),
      to: table(:functions, prefix: @config_schema)

    rename table(:functions), :transform, to: :function

    # Update JSONB column 'function' type key from 'function' to 'transform'
    execute """
            UPDATE #{@config_schema}.functions
            SET function = jsonb_set(function, '{type}', '"transform"'::jsonb)
            WHERE function->>'type' = 'function';
            """,
            """
            UPDATE #{@config_schema}.functions
            SET function = jsonb_set(function, '{type}', '"function"'::jsonb)
            WHERE function->>'type' = 'transform';
            """

    execute """
            create or replace function #{@config_schema}.check_routing_id_type()
            returns trigger as $$
            begin
              if new.routing_id is not null then
                if not exists (
                  select 1 from #{@config_schema}.functions
                  where id = new.routing_id and type = 'routing'
                ) then
                  raise exception 'routing_id must reference a function with type ''routing''';
                end if;
              end if;
              return new;
            end;
            $$ language plpgsql;
            """,
            """
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
            """
  end
end
