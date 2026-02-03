defmodule Sequin.Repo.Migrations.AddColumnSelectionToSourceTables do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    # Add column selection fields to WAL pipelines source_tables
    execute(
      """
      DO $$
      DECLARE
        wp RECORD;
        source_tables_updated jsonb;
        source_table jsonb;
      BEGIN
        FOR wp IN
          SELECT id, source_tables
          FROM #{@config_schema}.wal_pipelines
          WHERE source_tables IS NOT NULL
        LOOP
          source_tables_updated := '[]'::jsonb;

          FOR source_table IN
            SELECT * FROM jsonb_array_elements(wp.source_tables)
          LOOP
            source_tables_updated := source_tables_updated || jsonb_build_array(
              source_table || jsonb_build_object(
                'include_column_attnums', NULL,
                'exclude_column_attnums', NULL
              )
            );
          END LOOP;

          UPDATE #{@config_schema}.wal_pipelines
          SET source_tables = source_tables_updated
          WHERE id = wp.id;
        END LOOP;
      END $$;
      """,
      """
      DO $$
      DECLARE
        wp RECORD;
        source_tables_updated jsonb;
        source_table jsonb;
      BEGIN
        FOR wp IN
          SELECT id, source_tables
          FROM #{@config_schema}.wal_pipelines
          WHERE source_tables IS NOT NULL
        LOOP
          source_tables_updated := '[]'::jsonb;

          FOR source_table IN
            SELECT * FROM jsonb_array_elements(wp.source_tables)
          LOOP
            source_tables_updated := source_tables_updated || jsonb_build_array(
              source_table - 'include_column_attnums' - 'exclude_column_attnums'
            );
          END LOOP;

          UPDATE #{@config_schema}.wal_pipelines
          SET source_tables = source_tables_updated
          WHERE id = wp.id;
        END LOOP;
      END $$;
      """
    )
  end
end
