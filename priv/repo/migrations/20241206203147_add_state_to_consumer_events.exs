defmodule Sequin.Repo.Migrations.AddStateToConsumerEvents do
  use Ecto.Migration

  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def change do
    execute "create type #{@stream_schema}.consumer_event_state as enum ('available', 'delivered');",
            "drop type #{@stream_schema}.consumer_event_state"

    alter table(:consumer_events, prefix: @stream_schema) do
      add :state, :"#{@stream_schema}.consumer_event_state", default: "available"
    end

    execute "update #{@stream_schema}.consumer_events set state = 'available' where state is null",
            "select 1"

    alter table(:consumer_events, prefix: @stream_schema) do
      modify :state, :"#{@stream_schema}.consumer_event_state", null: false, default: "available"
    end
  end
end
