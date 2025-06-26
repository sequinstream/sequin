defmodule Sequin.Repo.Migrations.AddRecordPksToMetadata do
  use Ecto.Migration

  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def change do
    # Update consumer_records table
    execute """
    UPDATE #{@stream_schema}.consumer_records
    SET data = jsonb_set(
      data,
      '{metadata,record_pks}',
      to_jsonb(record_pks),
      true
    )
    WHERE record_pks IS NOT NULL
    """

    # Update consumer_events table
    execute """
    UPDATE #{@stream_schema}.consumer_events
    SET data = jsonb_set(
      data,
      '{metadata,record_pks}',
      to_jsonb(record_pks),
      true
    )
    WHERE record_pks IS NOT NULL
    """
  end
end
