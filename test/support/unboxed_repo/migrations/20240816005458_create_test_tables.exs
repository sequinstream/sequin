defmodule Sequin.Test.UnboxedRepo.Migrations.CreateTestTables do
  @moduledoc false
  use Ecto.Migration

  alias Sequin.Constants

  def my_version, do: 1

  def check_version!(repo) do
    if my_version() != get_version(repo) do
      IO.puts(
        :stderr,
        [
          IO.ANSI.red(),
          "UnboxedRepo version has been updated!",
          IO.ANSI.reset(),
          "\n",
          IO.ANSI.bright(),
          "Please run the following command to re-create:",
          IO.ANSI.reset(),
          "\n\n",
          "MIX_ENV=test mix ecto.reset\n"
        ]
      )

      System.halt(1)
    end
  end

  defp get_version(repo) do
    case repo.query("select my_version from my_version_info") do
      {:error, _} -> nil
      {:ok, %Postgrex.Result{rows: [[version]]}} -> version
    end
  end

  def change do
    execute "create table my_version_info as select #{my_version()} as my_version"

    create table(:Characters) do
      add :name, :text
      add :house, :text
      add :planet, :text
      add :is_active, :boolean
      add :tags, {:array, :text}
      add :metadata, :map

      timestamps(type: :naive_datetime_usec)
    end

    create table(:characters_ident_full) do
      add :name, :text
      add :house, :text
      add :planet, :text
      add :is_active, :boolean
      add :tags, {:array, :text}

      timestamps(type: :naive_datetime_usec)
    end

    execute "alter table characters_ident_full replica identity full"

    # New table with multiple primary keys of different types
    create table(:characters_multi_pk, primary_key: false) do
      add :id_integer, :serial, primary_key: true
      add :id_string, :string, primary_key: true
      add :id_uuid, :uuid, primary_key: true
      add :name, :text
      add :house, :text

      timestamps(type: :naive_datetime_usec)
    end

    execute "CREATE EXTENSION IF NOT EXISTS pg_trgm"
    execute "CREATE EXTENSION IF NOT EXISTS vector"
    execute "CREATE TYPE character_status AS ENUM ('active', 'inactive', 'retired')"

    # Add a domain type for positive integers
    execute "CREATE DOMAIN positive_int AS integer CHECK (VALUE > 0)"

    # New table with all possible column types
    create table(:characters_detailed) do
      add :name, :string
      add :status, :character_status
      # Using our domain type
      add :power_level, :positive_int
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
      add :binary_data, :bytea
      add :related_houses, {:array, :uuid}
      add :active_period, :daterange
      add :embedding, :vector, size: 3

      timestamps(type: :naive_datetime_usec)
    end

    # Older version without transaction_annotations
    create table(:test_event_logs_v0, primary_key: false) do
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
      add :committed_at, :naive_datetime_usec, null: false
      add :inserted_at, :naive_datetime_usec, null: false, default: fragment("NOW()")
    end

    create unique_index(:test_event_logs_v0, [:source_database_id, :committed_at, :seq, :record_pk])

    create index(:test_event_logs_v0, [:seq])
    create index(:test_event_logs_v0, [:source_table_oid])
    create index(:test_event_logs_v0, [:committed_at])

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
      add :transaction_annotations, :map
      add :committed_at, :naive_datetime_usec, null: false
      add :inserted_at, :naive_datetime_usec, null: false, default: fragment("NOW()")
    end

    create unique_index(:test_event_logs, [:source_database_id, :committed_at, :seq, :record_pk])
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
      add :committed_at, :naive_datetime_usec, null: false, primary_key: true
      add :inserted_at, :naive_datetime_usec, null: false, default: fragment("NOW()")
    end

    execute "CREATE TABLE test_event_logs_partitioned_default PARTITION OF test_event_logs_partitioned DEFAULT"

    create unique_index(:test_event_logs_partitioned, [:source_database_id, :committed_at, :seq, :record_pk])
    create index(:test_event_logs_partitioned, [:seq])
    create index(:test_event_logs_partitioned, [:source_table_oid])
    create index(:test_event_logs_partitioned, [:committed_at])

    execute "COMMENT ON TABLE test_event_logs_partitioned IS '$sequin-events$'"

    execute Sequin.Postgres.logical_messages_table_ddl(),
            "drop table if exists #{Constants.logical_messages_table_name()}"

    execute "create publication characters_publication for table \"Characters\", characters_ident_full, characters_multi_pk, characters_detailed, test_event_logs_partitioned, #{Constants.logical_messages_table_name()}",
            "drop publication characters_publication"

    create table(:sequin_events) do
      add :seq, :bigint, null: false
      add :source_database_id, :uuid, null: false
      add :source_table_oid, :bigint, null: false
      add :source_table_schema, :text, null: false
      add :source_table_name, :text, null: false
      add :record_pk, :text, null: false
      add :record, :map, null: false
      add :changes, :map
      add :action, :text, null: false
      add :committed_at, :naive_datetime_usec, null: false
      add :inserted_at, :naive_datetime_usec, null: false, default: fragment("NOW()")
    end

    create unique_index(:sequin_events, [:source_database_id, :committed_at, :seq, :record_pk])

    # Not included in the publication
    create table(:my_non_published_table) do
    end
  end
end
