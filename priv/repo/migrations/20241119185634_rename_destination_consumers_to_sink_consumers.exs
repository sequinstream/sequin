defmodule Sequin.Repo.Migrations.RenameDestinationConsumersToSinkConsumers do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    rename table(:destination_consumers, prefix: @config_schema),
      to: table(:sink_consumers, prefix: @config_schema)

    rename table(:sink_consumers, prefix: @config_schema), :destination, to: :sink
  end
end
