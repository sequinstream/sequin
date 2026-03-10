defmodule Sequin.Repo.Migrations.AddUseIamAuthToPostgresDatabases do
  use Ecto.Migration

  def change do
    alter table(:postgres_databases) do
      add :use_iam_auth, :boolean, default: false, null: false
      add :iam_region, :string
    end
  end
end
