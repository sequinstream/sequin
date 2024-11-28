defmodule Sequin.Repo.Migrations.AddDeploymentConfig do
  use Ecto.Migration
  @config_schema Application.compile_env!(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    create table(:deployment_config, primary_key: false, prefix: @config_schema) do
      add :key, :text, primary_key: true
      add :value, :jsonb, null: false
      timestamps()
    end
  end
end
