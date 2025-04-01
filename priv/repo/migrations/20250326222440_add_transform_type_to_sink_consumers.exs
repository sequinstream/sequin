defmodule Sequin.Repo.Migrations.AddTransformTypeToSinkConsumers do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    execute "create type #{@config_schema}.transform_type as enum ('none', 'record_only');",
            "drop type #{@config_schema}.transform_type"

    alter table(:sink_consumers, prefix: @config_schema) do
      add :transform, :"#{@config_schema}.transform_type", default: "none"
    end
  end
end
