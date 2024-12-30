defmodule Sequin.Repo.Migrations.AddDataToConsumerRecords do
  use Ecto.Migration

  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def change do
    alter table(:consumer_records, prefix: @stream_schema) do
      add :data, :map
    end
  end
end
