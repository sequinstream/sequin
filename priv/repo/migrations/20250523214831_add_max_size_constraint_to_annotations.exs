defmodule Sequin.Repo.Migrations.AddMaxSizeConstraintToAnnotations do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])
  @max_annotation_size_bytes 8192

  def change do
    # Add check constraints to limit the size of annotations
    # for postgres_databases table
    execute(
      "ALTER TABLE #{@config_schema}.postgres_databases ADD CONSTRAINT postgres_databases_annotations_size_limit CHECK (octet_length(annotations::text) <= #{@max_annotation_size_bytes})",
      "ALTER TABLE #{@config_schema}.postgres_databases DROP CONSTRAINT postgres_databases_annotations_size_limit"
    )

    # for postgres_replication_slots table
    execute(
      "ALTER TABLE #{@config_schema}.postgres_replication_slots ADD CONSTRAINT postgres_replication_slots_annotations_size_limit CHECK (octet_length(annotations::text) <= #{@max_annotation_size_bytes})",
      "ALTER TABLE #{@config_schema}.postgres_replication_slots DROP CONSTRAINT postgres_replication_slots_annotations_size_limit"
    )

    # for sink_consumers table
    execute(
      "ALTER TABLE #{@config_schema}.sink_consumers ADD CONSTRAINT sink_consumers_annotations_size_limit CHECK (octet_length(annotations::text) <= #{@max_annotation_size_bytes})",
      "ALTER TABLE #{@config_schema}.sink_consumers DROP CONSTRAINT sink_consumers_annotations_size_limit"
    )

    # for accounts table
    execute(
      "ALTER TABLE #{@config_schema}.accounts ADD CONSTRAINT accounts_annotations_size_limit CHECK (octet_length(annotations::text) <= #{@max_annotation_size_bytes})",
      "ALTER TABLE #{@config_schema}.accounts DROP CONSTRAINT accounts_annotations_size_limit"
    )

    # for wal_pipelines table
    execute(
      "ALTER TABLE #{@config_schema}.wal_pipelines ADD CONSTRAINT wal_pipelines_annotations_size_limit CHECK (octet_length(annotations::text) <= #{@max_annotation_size_bytes})",
      "ALTER TABLE #{@config_schema}.wal_pipelines DROP CONSTRAINT wal_pipelines_annotations_size_limit"
    )
  end
end
