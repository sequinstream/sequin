defmodule Sequin.Health.CheckSinkConfigurationWorker do
  @moduledoc false

  use Oban.Worker,
    queue: :health_checks,
    max_attempts: 1,
    unique: [
      period: :infinity,
      states: ~w(available scheduled retryable)a,
      timestamp: :scheduled_at
    ]

  alias Sequin.Consumers
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Consumers.Source
  alias Sequin.Databases
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Health.Event

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"sink_consumer_id" => sink_consumer_id}}) do
    event = %Event{slug: :sink_config_checked, status: :success}

    {:ok, %SinkConsumer{source: %Source{} = source} = consumer} =
      Consumers.get_sink_consumer(sink_consumer_id, :replication_slot)

    publication_name = consumer.replication_slot.publication_name

    {:ok, postgres_database} = Databases.get_cached_db(consumer.replication_slot.postgres_database_id)

    with {:ok, tables_with_replica_identities} <-
           Databases.all_tables_with_replica_identities_in_publication(postgres_database, publication_name),
         {:ok, pubviaroot} <- pubviaroot?(postgres_database, publication_name) do
      tables_with_replica_identities =
        Enum.filter(tables_with_replica_identities, fn %{"table_schema" => table_schema, "table_oid" => table_oid} ->
          Source.schema_and_table_oid_in_source?(source, table_schema, table_oid)
        end)

      Health.put_event(consumer, %Event{
        event
        | data: %{
            tables_with_replica_identities: tables_with_replica_identities,
            pubviaroot: pubviaroot
          }
      })
    else
      {:error, %Error.NotFoundError{entity: :sink_consumer}} ->
        Health.put_event(consumer, %Event{event | status: :fail, error: Error.not_found(entity: :sink_consumer)})

      {:error, error} when is_exception(error) ->
        Health.put_event(consumer, %Event{event | status: :fail, error: error})

      {:error, error} ->
        error =
          Error.service(
            service: :check_sink_configuration_worker,
            message: "Failed to check sink configuration",
            details: inspect(error)
          )

        Health.put_event(consumer, %Event{event | status: :fail, error: error})
    end
  end

  def enqueue(sink_consumer_id) do
    %{sink_consumer_id: sink_consumer_id}
    |> new()
    |> Oban.insert()
  end

  def enqueue_for_user(sink_consumer_id) do
    %{sink_consumer_id: sink_consumer_id}
    |> new(queue: :user_submitted)
    |> Oban.insert()
  end

  defp pubviaroot?(%PostgresDatabase{} = postgres_database, publication_name) do
    case Databases.get_publication(postgres_database, publication_name) do
      {:ok, %{"pubviaroot" => pubviaroot}} -> {:ok, pubviaroot}
      {:error, error} -> {:error, error}
    end
  end
end
