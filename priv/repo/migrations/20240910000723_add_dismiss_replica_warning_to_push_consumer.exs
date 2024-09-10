defmodule Sequin.Repo.Migrations.AddDismissReplicaWarningToPushConsumer do
  use Ecto.Migration

  def change do
    alter table(:http_push_consumers) do
      add :replica_warning_dismissed, :boolean, default: false, null: false
    end
  end
end
