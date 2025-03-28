defmodule Sequin.Repo.Migrations.AddTransformToSinkConsumers do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:sink_consumers, prefix: @config_schema) do
      add :transform_id, references(:transforms, on_delete: :restrict, prefix: @config_schema)
    end

    create index(:sink_consumers, [:transform_id], prefix: @config_schema)
  end
end
