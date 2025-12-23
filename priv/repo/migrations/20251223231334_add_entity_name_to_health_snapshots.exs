defmodule Sequin.Repo.Migrations.AddEntityNameToHealthSnapshots do
  use Ecto.Migration

  def change do
    alter table(:health_snapshots) do
      add :entity_name, :string
    end
  end
end
