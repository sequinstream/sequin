defmodule Sequin.Repo.Migrations.AddRecordConsumerStateToConsumers do
  use Ecto.Migration

  def change do
    alter table(:http_push_consumers) do
      add :record_consumer_state, :map
    end

    alter table(:http_pull_consumers) do
      add :record_consumer_state, :map
    end

    create constraint(:http_push_consumers, :record_consumer_state_required,
             check: "(message_kind != 'record') or (record_consumer_state is not null)"
           )

    create constraint(:http_pull_consumers, :record_consumer_state_required,
             check: "(message_kind != 'record') or (record_consumer_state is not null)"
           )
  end
end
