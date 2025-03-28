defmodule Sequin.Repo.Migrations.CreateTransformsTable do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    create table(:transforms, prefix: @config_schema) do
      add :name, :string, null: false
      add :type, :string, generated: "always as (config->>'type') stored"
      add :config, :jsonb, null: false

      add :account_id, references(:accounts, on_delete: :delete_all, prefix: @config_schema),
        null: false

      add :sequence_id, references(:sequences, on_delete: :delete_all, prefix: @config_schema),
        null: false

      timestamps()
    end

    create unique_index(:transforms, [:account_id, :name], prefix: @config_schema)
    create index(:transforms, [:account_id], prefix: @config_schema)
    create index(:transforms, [:sequence_id], prefix: @config_schema)
  end
end
