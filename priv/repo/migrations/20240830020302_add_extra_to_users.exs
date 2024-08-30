defmodule Sequin.Repo.Migrations.AddExtraToUsers do
  use Ecto.Migration

  def change do
    alter table(:users) do
      add :extra, :map
    end
  end
end
