defmodule Sequin.Repo.Migrations.CreatePostgresDatabasesPrimaries do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    create table(:postgres_databases_primaries, prefix: @config_schema) do
      add :database, :string, null: false
      add :hostname, :string, null: false
      add :password, :binary, null: false
      add :port, :integer, null: false
      add :ssl, :boolean, default: false, null: false
      add :username, :string, null: false
      add :ipv6, :boolean, default: false, null: false
      add :postgres_database_id, references(:postgres_databases, type: :uuid, on_delete: :delete_all), null: false

      timestamps()
    end

    create unique_index(:postgres_databases_primaries, [:postgres_database_id, :hostname])
  end

end
