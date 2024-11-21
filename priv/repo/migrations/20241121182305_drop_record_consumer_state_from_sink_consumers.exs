defmodule Sequin.Repo.Migrations.DropRecordConsumerStateFromSinkConsumers do
  use Ecto.Migration

  def change do
    alter table(:sink_consumers) do
      remove :record_consumer_state
    end
  end
end
