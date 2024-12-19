defmodule Sequin.Repo.Migrations.AlterSeqUniqueConstraint do
  use Ecto.Migration
  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def change do
    # Consumer Records table
    drop index(:consumer_records, [:consumer_id, :seq], prefix: @stream_schema)

    create index(:consumer_records, [:consumer_id, :seq],
             prefix: @stream_schema,
             name: :consumer_records_consumer_id_seq_index
           )

    create unique_index(:consumer_records, [:consumer_id, :seq, :record_pks],
             prefix: @stream_schema,
             name: :consumer_records_consumer_id_seq_pks_unique_index
           )

    # Consumer Events table
    drop index(:consumer_events, [:consumer_id, :seq], prefix: @stream_schema)

    create index(:consumer_events, [:consumer_id, :seq],
             prefix: @stream_schema,
             name: :consumer_events_consumer_id_seq_index
           )

    create unique_index(:consumer_events, [:consumer_id, :seq, :record_pks],
             prefix: @stream_schema,
             name: :consumer_events_consumer_id_seq_pks_unique_index
           )
  end
end
