defmodule Sequin.Repo.Migrations.DropSinkConsumersSourceTables do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    execute(
      "drop trigger if exists check_sort_column_attnum_push on #{@config_schema}.sink_consumers;"
    )

    execute("drop function if exists #{@config_schema}.check_sort_column_attnum();")

    alter table(:sink_consumers, prefix: @config_schema) do
      remove :source_tables
    end
  end

  def down do
    alter table(:sink_consumers, prefix: @config_schema) do
      add :source_tables, {:array, :map}, null: false, default: "{}"
    end

    execute("""
    create or replace function #{@config_schema}.check_sort_column_attnum()
    returns trigger as $$
    begin
      if new.message_kind = 'record' then
        if array_length(new.source_tables, 1) = 0 then
          raise exception 'source_tables must not be empty when message_kind is record';
        end if;

        if exists (
          select 1
          from unnest(new.source_tables) as st
          where (st->>'sort_column_attnum')::int is null
        ) then
          raise exception 'All source tables must have a sort_column_attnum when message_kind is record';
        end if;
      end if;
      return new;
    end;
    $$ language plpgsql;
    """)

    execute("""
    create trigger check_sort_column_attnum_push
    before insert or update on #{@config_schema}.sink_consumers
    for each row execute function #{@config_schema}.check_sort_column_attnum();
    """)
  end
end
