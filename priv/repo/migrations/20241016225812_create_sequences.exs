defmodule Sequin.Repo.Migrations.CreateSequences do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    create table(:sequences, prefix: @config_schema) do
      add :postgres_database_id,
          references(:postgres_databases, on_delete: :restrict, prefix: @config_schema),
          null: false

      add :table_oid, :integer, null: false
      add :table_schema, :string, null: true
      add :table_name, :string, null: true
      add :sort_column_attnum, :integer, null: false
      add :sort_column_name, :string, null: true

      timestamps()
    end

    create unique_index(:sequences, [:postgres_database_id, :table_oid])

    alter table(:http_push_consumers, prefix: @config_schema) do
      add :sequence_id,
          references(:sequences, on_delete: :restrict, prefix: @config_schema),
          null: true

      add :sequence_filter, :map, null: true
    end

    execute """
            ALTER TABLE #{@config_schema}.http_push_consumers
            ADD CONSTRAINT sequence_filter_check
            CHECK (
              (sequence_id IS NOT NULL AND sequence_filter IS NOT NULL) OR
              (sequence_id IS NULL AND sequence_filter IS NULL)
            );
            """,
            "select 1;"

    alter table(:http_pull_consumers, prefix: @config_schema) do
      add :sequence_id,
          references(:sequences, on_delete: :restrict, prefix: @config_schema),
          null: true

      add :sequence_filter, :map, null: true
    end

    execute """
            ALTER TABLE #{@config_schema}.http_pull_consumers
            ADD CONSTRAINT sequence_filter_check
            CHECK (
              (sequence_id IS NOT NULL AND sequence_filter IS NOT NULL) OR
              (sequence_id IS NULL AND sequence_filter IS NULL)
            );
            """,
            "select 1;"

    flush()

    Mix.Tasks.Sequin.MigrateToSequences.run([])
  end
end
