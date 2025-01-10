defmodule Sequin.Health.CheckSinkConfigurationWorker do
  @moduledoc false

  use Oban.Worker,
    queue: :default,
    max_attempts: 1,
    unique: [period: 120, timestamp: :scheduled_at]

  alias Sequin.Consumers
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases
  alias Sequin.Error
  alias Sequin.Health
  alias Sequin.Health.Event
  alias Sequin.Repo

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"sink_consumer_id" => sink_consumer_id}}) do
    with {:ok, consumer} <- Consumers.get_sink_consumer(sink_consumer_id),
         consumer = Repo.preload(consumer, [:postgres_database, :sequence]),
         {:ok, replica_identity} <- check_replica_identity(consumer),
         :ok <- check_publication(consumer) do
      Health.put_event(consumer, %Event{
        slug: :sink_config_checked,
        status: :success,
        data: %{replica_identity: replica_identity}
      })

      :syn.publish(:consumers, {:sink_config_checked, consumer.id}, :sink_config_checked)

      :ok
    else
      {:error, %Error.NotFoundError{}} ->
        :ok

      {:error, error} ->
        {:ok, consumer} = Consumers.get_sink_consumer(sink_consumer_id)

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

  defp check_replica_identity(%SinkConsumer{message_kind: :record}) do
    :ok
  end

  defp check_replica_identity(%SinkConsumer{message_kind: :event} = consumer) do
    source_table = Consumers.source_table(consumer)
    Databases.check_replica_identity(consumer.postgres_database, source_table.oid)
  end

  defp check_publication(%SinkConsumer{} = _consumer) do
    :ok
  end
end
