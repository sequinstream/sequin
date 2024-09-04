defmodule Sequin.Repo.Migrations.AddHttpEndpointPathToHttpPushConsumers do
  use Ecto.Migration

  def change do
    alter table(:http_push_consumers) do
      add :http_endpoint_path, :string, null: false, default: ""
    end
  end
end
