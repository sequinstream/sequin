defmodule Sequin.Repo.Migrations.PopulateMetadataDatabaseInConsumerEvents do
  use Ecto.Migration

  @stream_schema Application.compile_env(:sequin, [Sequin.Repo, :stream_schema_prefix])

  def up do
    # Update consumer_events that don't have metadata.database set
    execute """
    UPDATE #{@stream_schema}.consumer_events
    SET data = jsonb_set(
      data,
      '{metadata,database}',
      '{
        "id": "00000000-0000-0000-0000-000000000000",
        "name": "migrated_database",
        "annotations": {},
        "database": "migrated_db",
        "hostname": "migrated.host"
      }'::jsonb
    )
    WHERE data->'metadata'->>'database' IS NULL
       OR data->'metadata'->'database' IS NULL;
    """
  end

  def down do
    # Remove the metadata.database field that was added
    execute """
    UPDATE #{@stream_schema}.consumer_events
    SET data = data #- '{metadata,database}'
    WHERE data->'metadata'->'database'->>'id' = '00000000-0000-0000-0000-000000000000';
    """
  end
end
