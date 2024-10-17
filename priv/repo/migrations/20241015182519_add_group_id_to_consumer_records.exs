defmodule Sequin.Repo.Migrations.AddGroupIdToConsumerRecords do
  use Ecto.Migration

  @stream_schema Application.compile_env!(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def change do
    alter table(:consumer_records, prefix: @stream_schema) do
      add :group_id, :string
    end

    create index(:consumer_records, [:group_id], prefix: @stream_schema)
    create index(:consumer_records, [:consumer_id, :group_id, :table_oid], prefix: @stream_schema)

    create index(:consumer_records, [:consumer_id, :not_visible_until, :state],
             prefix: @stream_schema
           )
  end
end
