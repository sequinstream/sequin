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
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Postgres

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"sink_consumer_id" => sink_consumer_id}}) do
    event = %Event{slug: :sink_config_checked, status: :success}

    case Consumers.get_sink_consumer(sink_consumer_id, [:sequence, :replication_slot]) do
      {:ok, %SinkConsumer{source: %Source{}} = consumer} ->
        # FIXME: check configuration for all tables in source
        event = %Event{event | data: %{using_schema_filter: true}}
        Health.put_event(consumer, event)

        #   {:ok, %SinkConsumer{sequence: %Sequence{} = sequence} = consumer} ->
        #     with {:ok, postgres_database} <- Databases.get_cached_db(consumer.replication_slot.postgres_database_id),
        #          {:ok, relation_kind} <- Postgres.get_relation_kind(postgres_database, sequence.table_oid),
        #          {:ok, event} <- check_replica_identity(consumer, postgres_database, event, relation_kind),
        #          {:ok, event} <- check_publication(consumer, postgres_database, event, relation_kind) do
        #       Health.put_event(consumer, event)

        #       :syn.publish(:consumers, {:sink_config_checked, consumer.id}, :sink_config_checked)

        #       :ok
        #     else
        #       {:error, %Error.NotFoundError{entity: :table}} ->
        #         Health.put_event(consumer, %Event{
        #           slug: :sink_config_checked,
        #           error: Error.invariant(message: "Source table does not exist in database"),
        #           status: :fail
        #         })

        #         :ok

        #       {:error, error} ->
        #         Health.put_event(consumer, %Event{
        #           slug: :sink_config_checked,
        #           error:
        #             Error.service(
        #               service: :postgres_database,
        #               message: "Failed to run check against database: #{Exception.message(error)}"
        #             ),
        #           status: :fail
        #         })
        #     end

        #   {:error, %Error.NotFoundError{}} ->
        #     :ok
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

  defp check_replica_identity(%SinkConsumer{} = consumer, postgres_database, %Event{} = event, relation_kind) do
    source_table = Consumers.source_table(consumer, postgres_database)

    with {:ok, replica_identity} <-
           get_replica_identity_by_kind(relation_kind, postgres_database, source_table.oid) do
      {:ok, %Event{event | data: %{replica_identity: replica_identity, relation_kind: relation_kind}}}
    end
  end

  defp get_replica_identity_by_kind("p", postgres_database, table_oid) do
    # For partitioned tables, check all partitions
    Postgres.check_partitioned_replica_identity(postgres_database, table_oid)
  end

  defp get_replica_identity_by_kind(_relation_kind, postgres_database, table_oid) do
    # For regular tables and any other kind, just check the single table
    Databases.check_replica_identity(postgres_database, table_oid)
  end

  defp check_publication(%SinkConsumer{} = consumer, postgres_database, %Event{} = event, relation_kind) do
    slot = consumer.replication_slot

    with {:ok, publication} <- Postgres.get_publication(postgres_database, slot.publication_name),
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
