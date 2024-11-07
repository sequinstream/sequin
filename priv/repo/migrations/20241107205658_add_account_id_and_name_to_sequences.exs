defmodule Sequin.Repo.Migrations.AddAccountIdAndNameToSequences do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    alter table(:sequences, prefix: @config_schema) do
      add :account_id, references(:accounts, on_delete: :delete_all, prefix: @config_schema),
        null: true

      add :name, :string, null: true
    end

    # Set account_id based on database relationship
    execute("""
    UPDATE #{@config_schema}.sequences
    SET account_id = (
      SELECT account_id
      FROM #{@config_schema}.postgres_databases
      WHERE #{@config_schema}.postgres_databases.id = #{@config_schema}.sequences.postgres_database_id
    )
    """)

    # Set name using composed format: database_name.schema.table_name
    execute("""
    UPDATE #{@config_schema}.sequences s
    SET name = (
      SELECT pd.name || '.' || s2.table_schema || '.' || s2.table_name
      FROM #{@config_schema}.sequences s2
      JOIN #{@config_schema}.postgres_databases pd ON s2.postgres_database_id = pd.id
      WHERE s2.id = s.id
    )
    """)

    drop constraint(:sequences, "sequences_postgres_database_id_fkey", prefix: @config_schema)

    alter table(:sequences, prefix: @config_schema) do
      modify :account_id, :uuid, null: false
      modify :name, :string, null: false

      modify :postgres_database_id,
             references(:postgres_databases,
               with: [account_id: :account_id],
               prefix: @config_schema
             ),
             null: false
    end

    create unique_index(:sequences, [:account_id, :id], prefix: @config_schema)
    create unique_index(:sequences, [:account_id, :name], prefix: @config_schema)

    drop constraint(:http_pull_consumers, "http_pull_consumers_sequence_id_fkey",
           prefix: @config_schema
         )

    alter table(:http_pull_consumers, prefix: @config_schema) do
      modify :sequence_id,
             references(:sequences,
               with: [account_id: :account_id],
               prefix: @config_schema
             ),
             null: true
    end

    drop constraint(:http_push_consumers, "http_push_consumers_sequence_id_fkey",
           prefix: @config_schema
         )

    alter table(:http_push_consumers, prefix: @config_schema) do
      modify :sequence_id,
             references(:sequences,
               with: [account_id: :account_id],
               prefix: @config_schema
             ),
             null: true
    end
  end

  def down do
    # First drop the existing compound foreign key constraints
    drop constraint(:http_push_consumers, "http_push_consumers_sequence_id_fkey",
           prefix: @config_schema
         )

    drop constraint(:http_pull_consumers, "http_pull_consumers_sequence_id_fkey",
           prefix: @config_schema
         )

    # Then restore the original foreign key constraints for consumers
    alter table(:http_push_consumers, prefix: @config_schema) do
      modify :sequence_id, references(:sequences, prefix: @config_schema), null: false
    end

    alter table(:http_pull_consumers, prefix: @config_schema) do
      modify :sequence_id, references(:sequences, prefix: @config_schema), null: false
    end

    # Drop the unique indexes
    drop unique_index(:sequences, [:account_id, :id], prefix: @config_schema)
    drop unique_index(:sequences, [:account_id, :name], prefix: @config_schema)

    # Drop the compound foreign key for postgres_database_id
    drop constraint(:sequences, "sequences_postgres_database_id_fkey", prefix: @config_schema)

    # Restore original postgres_database_id foreign key
    alter table(:sequences, prefix: @config_schema) do
      modify :postgres_database_id, references(:postgres_databases, prefix: @config_schema),
        null: false
    end

    # Finally remove the new columns
    alter table(:sequences, prefix: @config_schema) do
      remove :account_id
      remove :name
    end
  end
end
