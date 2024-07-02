defmodule Sequin.Repo.Migrations.AddOban do
  use Ecto.Migration

  def up do
    Oban.Migrations.up()
  end

  def down do
    Oban.Migrations.down()
  end
end
