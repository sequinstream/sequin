defmodule Sequin.Repo.Migrations.AddSinkConsumerSource do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])
  def change do
    # First check if any sinks have column filters
    execute """
            DO $$
            DECLARE
              sink_count integer;
              sink_names text;
            BEGIN
              WITH sinks_with_filters AS (
                SELECT id, name
                FROM #{@config_schema}.sink_consumers
                WHERE sequence_filter->>'column_filters' IS NOT NULL
                  AND sequence_filter->>'column_filters' != '[]'
                  AND sequence_filter->>'column_filters' != 'null'
              )
              SELECT COUNT(*), string_agg(name, ', ')
              INTO sink_count, sink_names
              FROM sinks_with_filters;

              IF sink_count > 0 THEN
                RAISE EXCEPTION E'Cannot migrate: Found % sink(s) with column filters that need to be removed first: %\n\n'
                  'Column filters are deprecated in v0.10.0.\n'
                  'Please see the release notes for migration steps:\n'
                  'https://github.com/sequinstream/sequin/releases/tag/v0.10.0\n', sink_count, sink_names;
              END IF;
            END $$;
            """,
            "SELECT 1;"

    alter table(:sink_consumers, prefix: @config_schema) do
      add :actions, {:array, :string}, null: false, default: ["insert", "update", "delete"]
      add :source, :jsonb, null: true
      add :source_tables, {:array, :jsonb}, null: true
    end

    alter table(:sequences, prefix: @config_schema) do
      modify :postgres_database_id, references(:postgres_databases, on_delete: :delete_all),
        from: references(:postgres_databases, on_delete: :restrict)
    end

    # Migrate existing data
    execute """
            -- Update sinks with schema_filter
            UPDATE #{@config_schema}.sink_consumers
            SET
              actions = ARRAY['insert', 'update', 'delete'],
              source = jsonb_build_object('include_schemas', ARRAY[schema_filter->>'schema']),
              source_tables = ARRAY[]::jsonb[]
            WHERE schema_filter IS NOT NULL AND schema_filter->>'schema' IS NOT NULL;
            """,
            "SELECT 1;"

    execute """
            -- Update sinks with sequence_filter
            WITH sequence_data AS (
              SELECT
                sc.id as sink_consumer_id,
                s.table_oid,
                COALESCE(
                  (sc.sequence_filter->>'actions')::jsonb,
                  '["insert", "update", "delete"]'
                )::jsonb as actions,
                COALESCE(
                  (sc.sequence_filter->'group_column_attnums')::jsonb,
                  '[]'
                )::jsonb as group_column_attnums
              FROM #{@config_schema}.sink_consumers sc
              JOIN #{@config_schema}.sequences s ON s.id = sc.sequence_id
              WHERE sc.sequence_id IS NOT NULL
            )
            UPDATE #{@config_schema}.sink_consumers sc
            SET
              actions = (
                SELECT array_agg(value#>>'{}')
                FROM jsonb_array_elements(sd.actions)
              ),
              source = jsonb_build_object('include_table_oids', ARRAY[sd.table_oid]),
              source_tables = ARRAY[
                jsonb_build_object(
                  'table_oid', sd.table_oid,
                  'group_column_attnums', sd.group_column_attnums
                )
              ]
            FROM sequence_data sd
            WHERE sc.id = sd.sink_consumer_id;
            """,
            "SELECT 1;"
  end
end
