defmodule Sequin.Health.CheckSinkConfigurationWorker do
  @moduledoc false

  use Oban.Worker,
    queue: :default,
    max_attempts: 1,
    unique: [period: 30, timestamp: :scheduled_at]

  alias Sequin.Consumers
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Postgres
  alias Sequin.Repo

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"sink_consumer_id" => sink_consumer_id}}) do
    case Consumers.get_sink_consumer(sink_consumer_id) do
      {:ok, consumer} ->
        consumer = Repo.preload(consumer, [:postgres_database, :sequence, :replication_slot])

        with {:ok, tables} <- Databases.tables(consumer.postgres_database),
             :ok <- check_table_exists(tables, consumer.sequence.table_oid),
             {:ok, event} <- check_replica_identity(consumer),
             {:ok, event} <- check_publication(consumer, event) do
          Health.put_event(consumer, event)

          :syn.publish(:consumers, {:sink_config_checked, consumer.id}, :sink_config_checked)

          :ok
        else
          {:error, %Error.NotFoundError{entity: :table}} ->
            Health.put_event(consumer, %Event{
              slug: :sink_config_checked,
              error: Error.invariant(message: "Source table does not exist in database"),
              status: :fail
            })

            :ok

          {:error, error} ->
            Health.put_event(consumer, %Event{
              slug: :sink_config_checked,
              error:
                Error.service(
                  service: :postgres_database,
                  message: "Failed to run check against database: #{Exception.message(error)}"
                ),
              status: :fail
            })
        end

      {:error, %Error.NotFoundError{}} ->
        :ok
    end
  end

  def enqueue(sink_consumer_id) do
    %{sink_consumer_id: sink_consumer_id}
    |> new()
    |> Oban.insert()
  end

  def enqueue(sink_consumer_id, unique: false) do
    %{sink_consumer_id: sink_consumer_id}
    # Effectively disable unique constraint for this job
    |> new(unique: [states: [:available], period: 1])
    |> Oban.insert()
  end

  def enqueue_in(sink_consumer_id, delay_seconds) do
    %{sink_consumer_id: sink_consumer_id}
    |> new(schedule_in: delay_seconds)
    |> Oban.insert()
  end

  defp check_table_exists(tables, table_oid) do
    case Enum.find(tables, fn t -> t.oid == table_oid end) do
      nil -> {:error, Error.not_found(entity: :table, params: %{oid: table_oid})}
      _ -> :ok
    end
  end

  defp check_replica_identity(%SinkConsumer{} = consumer) do
    source_table = Consumers.source_table(consumer)

    with {:ok, replica_identity} <- Databases.check_replica_identity(consumer.postgres_database, source_table.oid) do
      {:ok, %Event{slug: :sink_config_checked, status: :success, data: %{replica_identity: replica_identity}}}
    end
  end

  defp check_publication(%SinkConsumer{} = consumer, %Event{} = event) do
    postgres_database = consumer.postgres_database
    slot = consumer.replication_slot

    with {:ok, relation_kind} <- Postgres.get_relation_kind(postgres_database, consumer.sequence.table_oid),
         {:ok, publication} <- Postgres.get_publication(postgres_database, slot.publication_name),
         :ok <- validate_partition_publication(relation_kind, publication),
         :ok <-
           Postgres.verify_table_in_publication(postgres_database, slot.publication_name, consumer.sequence.table_oid) do
      {:ok, event}
    else
      {:error, :partition_misconfigured} ->
        error =
          Error.invariant(
            message: "Partitioned table detected but publication is not configured with publish_via_partition_root=true"
          )

        {:ok, %{event | status: :fail, error: error}}

      {:error, %Error.NotFoundError{entity: :publication_membership} = error} ->
        {:ok, %{event | status: :fail, error: error}}

      {:error, %Error.NotFoundError{entity: :publication} = error} ->
        {:ok, %{event | status: :fail, error: error}}

      error ->
        error
    end
  end

  defp validate_partition_publication("p", %{"pubviaroot" => false}) do
    {:error, :partition_misconfigured}
  end

  defp validate_partition_publication(_, _), do: :ok
end
