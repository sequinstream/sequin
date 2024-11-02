defmodule Sequin.Test.UnboxedRepo.Migrations.CreateTestTables do
  @moduledoc false
  use Ecto.Migration

  def change do
    create table(:Characters) do
      add :name, :text
      add :house, :text
      add :planet, :text
      add :is_active, :boolean
      add :tags, {:array, :text}

      timestamps()
    end

    create table(:characters_ident_full) do
      add :name, :text
      add :house, :text
      add :planet, :text
      add :is_active, :boolean
      add :tags, {:array, :text}

      timestamps()
    end

    execute "alter table characters_ident_full replica identity full"

    # New table with multiple primary keys of different types
    create table(:characters_multi_pk, primary_key: false) do
      add :id_integer, :serial, primary_key: true
      add :id_string, :string, primary_key: true
      add :id_uuid, :uuid, primary_key: true
      add :name, :text
      add :house, :text

      timestamps()
    end

    execute "CREATE EXTENSION IF NOT EXISTS pg_trgm"

    # New table with all possible column types
    create table(:characters_detailed) do
      add :name, :string
      add :age, :integer
      add :height, :float
      add :is_hero, :boolean
      add :biography, :text
      add :biography_tsv, :tsvector, generated: "ALWAYS AS (to_tsvector('english', coalesce(biography, ''))) STORED"
      add :birth_date, :date
      add :last_seen, :time
      add :powers, {:array, :string}
      add :metadata, :map
      add :rating, :decimal
      add :avatar, :binary
      add :house_id, :uuid
      add :net_worth, :money
      add :email, :citext

      timestamps()
    end

    create table(:test_event_logs, primary_key: false) do
      add :id, :serial, primary_key: true
      add :seq, :bigint, null: false
      add :source_database_id, :uuid, null: false
      add :source_table_oid, :bigint, null: false
      add :source_table_schema, :text, null: false
      add :source_table_name, :text, null: false
      add :record_pk, :text, null: false
      add :record, :map, null: false
      add :changes, :map
      add :action, :string, null: false
      add :committed_at, :utc_datetime, null: false
      add :inserted_at, :utc_datetime, null: false, default: fragment("NOW()")
    end

    create unique_index(:test_event_logs, [:source_database_id, :seq, :record_pk])
    create index(:test_event_logs, [:seq])
    create index(:test_event_logs, [:source_table_oid])
    create index(:test_event_logs, [:committed_at])

    # Simulate a pg_partman partitioned table
    create table(:test_event_logs_partitioned, primary_key: false, options: "PARTITION BY RANGE (committed_at)") do
      add :id, :bigserial, null: false, primary_key: true
      add :seq, :bigint, null: false
      add :source_database_id, :uuid, null: false
      add :source_table_oid, :bigint, null: false
      add :source_table_schema, :text, null: false
      add :source_table_name, :text, null: false
      add :record_pk, :text, null: false
      add :record, :map, null: false
      add :changes, :map
      add :action, :string, null: false
      add :committed_at, :utc_datetime, null: false, primary_key: true
      add :inserted_at, :utc_datetime, null: false, default: fragment("NOW()")
    end

    execute "CREATE TABLE test_event_logs_partitioned_default PARTITION OF test_event_logs_partitioned DEFAULT"

    create unique_index(:test_event_logs_partitioned, [:source_database_id, :committed_at, :seq, :record_pk])
    create index(:test_event_logs_partitioned, [:seq])
    create index(:test_event_logs_partitioned, [:source_table_oid])
    create index(:test_event_logs_partitioned, [:committed_at])

    execute "COMMENT ON TABLE test_event_logs_partitioned IS '$sequin-events$'"

    execute "create publication characters_publication for table \"Characters\", characters_ident_full, characters_multi_pk, characters_detailed, test_event_logs_partitioned",
            "drop publication characters_publication"
  end
end
