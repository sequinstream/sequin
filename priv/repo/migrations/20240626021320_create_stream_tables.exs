defmodule Sequin.Repo.Migrations.CreateStreamTables do
  use Ecto.Migration

  def change do
    execute "CREATE EXTENSION IF NOT EXISTS \"uuid-ossp\";", "select 1;"

    create table(:accounts) do
      timestamps()
    end

    execute "create sequence if not exists stream_idx_seq start with 1 increment by 1",
            "drop sequence if exists stream_idx_seq"

    create table(:streams) do
      add :idx, :bigint, default: fragment("nextval('stream_idx_seq')"), null: false
      add :account_id, references(:accounts, on_delete: :delete_all), null: false

      timestamps()
    end

    execute "create schema if not exists streams", "drop schema if exists streams"

    create table(:records,
             primary_key: false,
             prefix: "streams",
             options: "PARTITION BY LIST (stream_id)"
           ) do
      add :stream_id, :uuid, null: false, primary_key: true
      add :id, :uuid, null: false, primary_key: true
      add :data, :text, null: false
      add :data_hash, :text, null: false

      timestamps(type: :utc_datetime_usec)
    end
  end
end
