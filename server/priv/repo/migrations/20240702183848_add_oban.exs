defmodule Sequin.Repo.Migrations.AddOban do
  use Ecto.Migration

  @prefix Application.compile_env!(:sequin, [Sequin.Repo, :config_schema_prefix])

  def up do
    Oban.Migrations.up(prefix: @prefix)
  end

  def down do
    Oban.Migrations.down(prefix: @prefix)
  end
end
