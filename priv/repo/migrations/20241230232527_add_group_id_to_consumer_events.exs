defmodule Sequin.Repo.Migrations.AddGroupIdToConsumerEvents do
  use Ecto.Migration

  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def change do
    alter table(:consumer_events, prefix: @stream_schema) do
      add :group_id, :string
    end

    # Add composite indexes to optimize the query
    create index(:consumer_events, [:consumer_id, :group_id],
             prefix: @stream_schema,
             where: "group_id IS NOT NULL"
           )

    create index(:consumer_events, [:consumer_id, :not_visible_until], prefix: @stream_schema)
  end
end
