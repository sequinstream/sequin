defmodule Sequin.Repo.Migrations.CreateWalProjections do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    create table(:wal_projections, prefix: @config_schema) do
      add :name, :string, null: false
      add :seq, :integer, null: false
      add :source_tables, {:array, :jsonb}, null: false, default: "{}"
      add :destination_oid, :bigint

      add :replication_slot_id,
          references(:postgres_replication_slots,
            with: [account_id: :account_id],
            prefix: @config_schema
          ),
          null: false

      add :destination_database_id,
          references(:postgres_databases,
            with: [account_id: :account_id],
            prefix: @config_schema
          ),
          null: false

      add :account_id,
          references(:accounts, prefix: @config_schema),
          null: false

      timestamps()
    end

    create index(:wal_projections, [:replication_slot_id], prefix: @config_schema)
    create unique_index(:wal_projections, [:replication_slot_id, :name], prefix: @config_schema)

    execute "alter table #{@config_schema}.wal_projections alter column seq set default nextval('#{@config_schema}.consumer_seq');"
  end

  def down do
    drop table(:wal_projections, prefix: @config_schema)
  end
end
