defmodule Sequin.Repo.Migrations.AddMaxRetryCountToSinkConsumers do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:sink_consumers, prefix: @config_schema) do
      add :max_retry_count, :integer, default: nil
    end
  end
end
