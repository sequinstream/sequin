defmodule Sequin.Repo.Migrations.RenameConfigToTransformOnTransformsTable do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    rename table(:transforms, prefix: @config_schema), :config, to: :transform
  end
end
