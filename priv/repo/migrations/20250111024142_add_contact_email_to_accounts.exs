defmodule Sequin.Repo.Migrations.AddContactEmailToAccounts do
  use Ecto.Migration

  @config_schema Application.compile_env(:sequin, [Sequin.Repo, :config_schema_prefix])

  def change do
    alter table(:accounts, prefix: @config_schema) do
      add :contact_email, :string
    end
  end
end
