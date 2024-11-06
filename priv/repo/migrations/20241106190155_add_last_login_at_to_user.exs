defmodule Sequin.Repo.Migrations.AddLastLoginAtToUser do
  use Ecto.Migration

  def change do
    alter table(:users) do
      add :last_login_at, :utc_datetime
    end
  end
end
