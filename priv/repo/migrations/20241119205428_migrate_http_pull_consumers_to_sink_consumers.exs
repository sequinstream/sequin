defmodule Sequin.Repo.Migrations.MigrateHttpPullConsumersToSinkConsumers do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    execute(
      """
      INSERT INTO #{@config_schema}.sink_consumers (
        id, name, backfill_completed_at, ack_wait_ms, max_ack_pending, max_deliver,
        max_waiting, message_kind, status, seq, account_id, replication_slot_id,
        sequence_id, source_tables, record_consumer_state, sequence_filter,
        inserted_at, updated_at, batch_size, sink
      )
      SELECT
        id, name, backfill_completed_at, ack_wait_ms, max_ack_pending, max_deliver,
        max_waiting, message_kind::#{@config_schema}.consumer_message_kind, status, seq, account_id, replication_slot_id,
        sequence_id, source_tables, record_consumer_state, sequence_filter,
        inserted_at, updated_at, 1, jsonb_build_object('type', 'sequin_stream')
      FROM #{@config_schema}.http_pull_consumers
      """,
      """
      DELETE FROM #{@config_schema}.sink_consumers
      WHERE id IN (SELECT id FROM #{@config_schema}.http_pull_consumers)
      """
    )
  end
end
