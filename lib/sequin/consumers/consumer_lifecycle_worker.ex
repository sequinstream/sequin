defmodule Sequin.Consumers.ConsumerLifecycleWorker do
  @moduledoc false
  use Oban.Worker, queue: :default, max_attempts: 1

  alias Sequin.Consumers
  alias Sequin.Repo

  @type lifecycle_event :: :create | :delete | :update

  # Public API for enqueueing jobs
  def enqueue_create(consumer_id) do
    Oban.insert(%{consumer_id: consumer_id, event: "create"})
  end

  def enqueue_delete(consumer_id) do
    Oban.insert(%{consumer_id: consumer_id, event: "delete"})
  end

  def enqueue_update(consumer_id) do
    Oban.insert(%{consumer_id: consumer_id, event: "update"})
  end

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"consumer_id" => consumer_id, "event" => event}}) do
    Logger.metadata(consumer_id: consumer_id)

    with {:ok, consumer} <- Consumers.get_consumer(consumer_id) do
      consumer = Repo.preload(consumer, :active_backfill)

      case event do
        "create" -> Consumers.handle_consumer_create(consumer)
        "delete" -> Consumers.handle_consumer_delete(consumer)
        "update" -> Consumers.handle_consumer_update(consumer)
      end
    end
  end
end
