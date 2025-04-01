defmodule Sequin.Repo.Migrations.DropSequenceIdFromTransforms do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:transforms, prefix: @config_schema) do
      remove :sequence_id
    end
  end
end
