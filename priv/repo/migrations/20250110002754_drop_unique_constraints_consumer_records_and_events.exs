defmodule Sequin.Repo.Migrations.DropUniqueConstraintsConsumerRecordsAndEvents do
  use Ecto.Migration

  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def change do
    drop(
      unique_index(:consumer_records, [:consumer_id, :seq, :record_pks],
        name: "consumer_records_consumer_id_seq_pks_unique_index",
        prefix: @stream_schema
      )
    )

    drop(
      unique_index(:consumer_records, [:consumer_id, :record_pks, :table_oid],
        prefix: @stream_schema
      )
    )

    drop(
      unique_index(:consumer_events, [:consumer_id, :seq, :record_pks],
        name: "consumer_events_consumer_id_seq_pks_unique_index",
        prefix: @stream_schema
      )
    )
  end
end
