defmodule Sequin.Repo.Migrations.AddPayloadSizeBytesToConsumerTables do
  use Ecto.Migration

  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def change do
    alter table(:consumer_records, prefix: @stream_schema) do
      add :payload_size_bytes, :integer
    end

    alter table(:consumer_events, prefix: @stream_schema) do
      add :payload_size_bytes, :integer
    end
  end
end
