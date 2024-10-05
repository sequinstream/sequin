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

      timestamps()
    end

    # New table with all possible column types
    create table(:characters_detailed) do
      add :name, :string
      add :age, :integer
      add :height, :float
      add :is_hero, :boolean
      add :biography, :text
      add :birth_date, :date
      add :last_seen, :time
      add :powers, {:array, :string}
      add :metadata, :map
      add :rating, :decimal
      add :avatar, :binary

      timestamps()
    end

    create table(:test_event_logs) do
      add :seq, :bigint, null: false
      add :source_database_id, :uuid, null: false
      add :source_oid, :bigint, null: false
      add :source_pk, :text, null: false
      add :record, :map, null: false
      add :changes, :map
      add :action, :string, null: false
      add :committed_at, :utc_datetime_usec, null: false
      add :inserted_at, :utc_datetime_usec, null: false, default: fragment("NOW()")
    end

    create unique_index(:test_event_logs, [:seq, :source_database_id])
    create index(:test_event_logs, [:source_oid])
    create index(:test_event_logs, [:committed_at])

    execute "create publication characters_publication for table \"Characters\", characters_ident_full, characters_multi_pk, characters_detailed",
            "drop publication characters_publication"
  end
end
