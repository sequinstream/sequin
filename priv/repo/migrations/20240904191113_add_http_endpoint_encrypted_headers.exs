defmodule Sequin.Repo.Migrations.AddHttpEndpointEncryptedHeaders do
  use Ecto.Migration

  def change do
    alter table(:http_endpoints) do
      add :encrypted_headers, :binary
    end
  end
end
