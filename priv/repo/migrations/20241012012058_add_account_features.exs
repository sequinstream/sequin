defmodule Sequin.Repo.Migrations.AddAccountFeatures do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:accounts, prefix: @config_schema) do
      add :features, {:array, :text}, default: []
    end
  end
end
