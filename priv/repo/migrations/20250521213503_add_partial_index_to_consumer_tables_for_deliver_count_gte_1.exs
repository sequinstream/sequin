defmodule Sequin.Repo.Migrations.AddPartialIndexToConsumerTablesForDeliverCountGte1 do
  use Ecto.Migration
  @disable_ddl_transaction true

  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def up do
    create_if_not_exists index(:consumer_events, [:consumer_id],
                           prefix: @stream_schema,
                           where: "deliver_count >= 1",
                           name: :consumer_events_deliver_count_gte_1_index
                         )

    create_if_not_exists index(:consumer_records, [:consumer_id],
                           prefix: @stream_schema,
                           where: "deliver_count >= 1",
                           name: :consumer_records_deliver_count_gte_1_index
                         )
  end

  def down do
    drop index(:consumer_events, [:consumer_id],
           prefix: @stream_schema,
           name: :consumer_events_deliver_count_gte_1_index
         )

    drop index(:consumer_records, [:consumer_id],
           prefix: @stream_schema,
           name: :consumer_records_deliver_count_gte_1_index
         )
  end
end
