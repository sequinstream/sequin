defmodule Sequin.Repo.Migrations.AddIpv6ToPostgresDatabase do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:postgres_databases, prefix: @config_schema) do
      add :ipv6, :boolean, default: false, null: false
    end
  end
end
