defmodule Sequin.Consumers do
  @moduledoc false
  import Ecto.Query

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.Query
  alias Sequin.Error
  alias Sequin.Repo
  alias Sequin.Streams.Consumer

  require Logger

  def reload(%ConsumerEvent{} = ce) do
    ce.consumer_id
    |> ConsumerEvent.where_consumer_id()
    |> ConsumerEvent.where_commit_lsn(ce.commit_lsn)
    |> Repo.one()
  end

  def list_consumer_events_for_consumer(consumer_id, params \\ []) do
    base_query = ConsumerEvent.where_consumer_id(consumer_id)

    query =
      Enum.reduce(params, base_query, fn
        {:is_deliverable, false}, query ->
          ConsumerEvent.where_not_visible(query)

        {:is_deliverable, true}, query ->
          ConsumerEvent.where_deliverable(query)

        {:limit, limit}, query ->
          limit(query, ^limit)

        {:order_by, order_by}, query ->
          order_by(query, ^order_by)
      end)

    Repo.all(query)
  end

  def get_consumer_event(consumer_id, commit_lsn) do
    consumer_event =
      consumer_id
      |> ConsumerEvent.where_consumer_id()
      |> ConsumerEvent.where_commit_lsn(commit_lsn)
      |> Repo.one()

    case consumer_event do
      nil -> {:error, Error.not_found(entity: :consumer_event)}
      consumer_event -> {:ok, consumer_event}
    end
  end

  def get_consumer_event!(consumer_id, commit_lsn) do
    case get_consumer_event(consumer_id, commit_lsn) do
      {:ok, consumer_event} -> consumer_event
      {:error, _} -> raise Error.not_found(entity: :consumer_event)
    end
  end

  def receive_for_consumer(%Consumer{message_kind: :event} = consumer, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, 100)
    not_visible_until = DateTime.add(DateTime.utc_now(), consumer.ack_wait_ms, :millisecond)
    now = NaiveDateTime.utc_now()
    max_ack_pending = consumer.max_ack_pending

    {:ok, events} =
      Query.receive_consumer_events(
        batch_size: batch_size,
        consumer_id: UUID.string_to_binary!(consumer.id),
        max_ack_pending: max_ack_pending,
        not_visible_until: not_visible_until,
        now: now
      )

    events =
      Enum.map(events, fn event ->
        event
        |> Map.update!(:consumer_id, &UUID.binary_to_string!/1)
        |> Map.update!(:inserted_at, &DateTime.from_naive!(&1, "Etc/UTC"))
        |> Map.update!(:updated_at, &DateTime.from_naive!(&1, "Etc/UTC"))
        |> Map.update!(:last_delivered_at, &DateTime.from_naive!(&1, "Etc/UTC"))
        |> then(&struct!(ConsumerEvent, &1))
      end)

    {:ok, events}
  end

  def insert_consumer_events([]), do: {:ok, []}

  def insert_consumer_events(consumer_events) do
    entries =
      Enum.map(consumer_events, fn event ->
        %{
          event
          | inserted_at: NaiveDateTime.utc_now(),
            updated_at: NaiveDateTime.utc_now()
        }
      end)

    {count, _} = Repo.insert_all(ConsumerEvent, entries)
    {:ok, count}
  end

  @spec ack_event_messages(Sequin.Streams.Consumer.t(), [integer()]) :: :ok
  def ack_event_messages(%Consumer{} = consumer, commit_lsns) do
    Repo.transact(fn ->
      {_, _} =
        consumer.id
        |> ConsumerEvent.where_consumer_id()
        |> ConsumerEvent.where_commit_lsns(commit_lsns)
        |> Repo.delete_all()

      :ok
    end)

    :ok
  end

  @spec nack_event_messages(Sequin.Streams.Consumer.t(), [integer()]) :: :ok
  def nack_event_messages(%Consumer{} = consumer, commit_lsns) do
    {_, _} =
      consumer.id
      |> ConsumerEvent.where_consumer_id()
      |> ConsumerEvent.where_commit_lsns(commit_lsns)
      |> Repo.update_all(set: [not_visible_until: nil])

    :ok
  end
end
