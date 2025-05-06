defmodule Sequin.Repo.Migrations.AddConstraintToSinkConsumersBatchTimeoutMs do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])
  def change do
    execute(
      "ALTER TABLE #{@config_schema}.sink_consumers ADD CONSTRAINT batch_timeout_ms_positive CHECK (batch_timeout_ms > 0);",
      "ALTER TABLE #{@config_schema}.sink_consumers DROP CONSTRAINT batch_timeout_ms_positive;"
    )
  end
end
