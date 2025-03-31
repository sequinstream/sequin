defmodule Sequin.Repo.Migrations.RenameTransformToLegacyTransform do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    rename(table(:sink_consumers, prefix: @config_schema), :transform, to: :legacy_transform)
  end
end
