defmodule Sequin.Repo.Migrations.ModifyTableOidToBigInt do
  use Ecto.Migration

  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])
  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    # consumer_events table
    alter table(:consumer_events, prefix: @stream_schema) do
      modify :table_oid, :bigint, null: false
    end

    # consumer_records table
    alter table(:consumer_records, prefix: @stream_schema) do
      modify :table_oid, :bigint, null: false
    end

    # sequences table
    alter table(:sequences, prefix: @config_schema) do
      modify :table_oid, :bigint, null: false
    end
  end
end
