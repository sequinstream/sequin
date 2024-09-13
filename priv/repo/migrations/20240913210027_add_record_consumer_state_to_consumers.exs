defmodule Sequin.Repo.Migrations.AddRecordConsumerStateToConsumers do
  use Ecto.Migration

  @config_schema_prefix Application.compile_env!(:sequin, Sequin.Repo)
                        |> Keyword.fetch!(:config_schema_prefix)

  def change do
    alter table(:http_push_consumers) do
      add :record_consumer_state, :map
    end

    alter table(:http_pull_consumers) do
      add :record_consumer_state, :map
    end

    execute """
    update #{@config_schema_prefix}.http_push_consumers
    set record_consumer_state = '{"producer": "wal"}'
    where message_kind = 'record'
    """

    execute """
    update #{@config_schema_prefix}.http_pull_consumers
    set record_consumer_state = '{"producer": "wal"}'
    where message_kind = 'record'
    """

    create constraint(:http_push_consumers, :record_consumer_state_required,
             check: "(message_kind != 'record') or (record_consumer_state is not null)"
           )

    create constraint(:http_pull_consumers, :record_consumer_state_required,
             check: "(message_kind != 'record') or (record_consumer_state is not null)"
           )

    create index(:http_push_consumers, ["(record_consumer_state->>'producer')"],
             name: :http_push_consumers_record_consumer_state_producer_index
           )

    create index(:http_pull_consumers, ["(record_consumer_state->>'producer')"],
             name: :http_pull_consumers_record_consumer_state_producer_index
           )
  end
end
