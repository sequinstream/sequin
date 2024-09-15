defmodule Sequin.Repo.Migrations.AddSortColumnAttnumConstraintToConsumers do
  use Ecto.Migration

  @config_schema_prefix Application.compile_env!(:sequin, Sequin.Repo)
                        |> Keyword.fetch!(:config_schema_prefix)

  def up do
    # Create the trigger function
    execute("""
    create or replace function #{@config_schema_prefix}.check_sort_column_attnum()
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

    # Create triggers for http_pull_consumers
    execute("""
    create trigger check_sort_column_attnum_pull
    before insert or update on #{@config_schema_prefix}.http_pull_consumers
    for each row execute function #{@config_schema_prefix}.check_sort_column_attnum();
    """)

    # Create triggers for http_push_consumers
    execute("""
    create trigger check_sort_column_attnum_push
    before insert or update on #{@config_schema_prefix}.http_push_consumers
    for each row execute function #{@config_schema_prefix}.check_sort_column_attnum();
    """)
  end

  def down do
    # Drop triggers
    execute(
      "drop trigger if exists check_sort_column_attnum_pull on #{@config_schema_prefix}.http_pull_consumers;"
    )

    execute(
      "drop trigger if exists check_sort_column_attnum_push on #{@config_schema_prefix}.http_push_consumers;"
    )

    # Drop the function
    execute("drop function if exists #{@config_schema_prefix}.check_sort_column_attnum();")
  end
end
