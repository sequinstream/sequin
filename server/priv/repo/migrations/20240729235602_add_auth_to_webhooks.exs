defmodule Sequin.Repo.Migrations.AddAuthToWebhooks do
  use Ecto.Migration

  def change do
    alter table(:webhooks) do
      add :auth_strategy, :map
    end
  end
end
