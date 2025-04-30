defmodule Sequin.Repo.Migrations.AddBatchTimeoutMsToSinkConsumers do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:sink_consumers, prefix: @config_schema) do
      add :batch_timeout_ms, :integer, default: nil
    end
  end
end
