defmodule Sequin.Repo.Migrations.CreateApiToken do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    create table(:api_tokens, prefix: @config_schema) do
      add :account_id, references(:accounts, on_delete: :delete_all, prefix: @config_schema),
        null: false

      add :name, :string, null: false
      add :token, :binary, null: false
      add :hashed_token, :binary, null: false

      timestamps(updated_at: false)
    end

    create index(:api_tokens, [:account_id], prefix: @config_schema)
    create unique_index(:api_tokens, [:account_id, :name], prefix: @config_schema)
  end
end
