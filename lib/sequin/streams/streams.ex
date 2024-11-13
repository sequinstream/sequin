defmodule Sequin.Streams do
  @moduledoc false
  import Ecto.Query

  alias Sequin.Consumers.DestinationConsumer
  alias Sequin.Error
  alias Sequin.Repo
  alias Sequin.Streams.ConsumerMessage
  alias Sequin.Streams.Message
  alias Sequin.Streams.Stream

  require Logger

  @stream_schema Application.compile_env!(:sequin, [Sequin.Repo, :stream_schema_prefix])
  @config_schema Application.compile_env!(:sequin, [Sequin.Repo, :config_schema_prefix])

  def stream_schema, do: @stream_schema
  def config_schema, do: @config_schema

  # General

  def reload(%Message{} = msg) do
    # Repo.reload/2 does not support compound pks
    msg.key |> Message.where_key_and_stream_id(msg.stream_id) |> Repo.one()
  end

  def reload(%ConsumerMessage{} = cm) do
    cm.consumer_id
    |> ConsumerMessage.where_consumer_id()
    |> ConsumerMessage.where_message_key(cm.message_key)
    |> Repo.one()
  end

  # Streams

  def list_streams_for_account(account_id) do
    account_id |> Stream.where_account_id() |> Stream.order_by(desc: :inserted_at) |> Repo.all()
  end

  def get_stream_for_account(account_id, id_or_name) do
    res = account_id |> Stream.where_account_id() |> Stream.where_id_or_name(id_or_name) |> Repo.one()

    case res do
      nil -> {:error, Error.not_found(entity: :stream)}
      stream -> {:ok, stream}
    end
  end

  def create_stream_for_account_with_lifecycle(account_id, attrs) do
    Repo.transaction(fn ->
      case create_stream(account_id, attrs) do
        {:ok, stream} ->
          create_records_partition(stream)
          stream

        {:error, changes} ->
          Repo.rollback(changes)
      end
    end)
  end

  def delete_stream_with_lifecycle(%Stream{} = stream) do
    Repo.transaction(fn ->
      case delete_stream(stream) do
        {:ok, stream} ->
          drop_records_partition(stream)
          stream

        {:error, changes} ->
          Repo.rollback(changes)
      end
    end)
  end

  def all_streams, do: Repo.all(Stream)

  def create_stream(account_id, attrs) do
    %Stream{account_id: account_id}
    |> Stream.changeset(attrs)
    |> Repo.insert()
  end

  defp create_records_partition(%Stream{} = stream) do
    Repo.query!("""
    CREATE TABLE #{stream_schema()}.messages_#{stream.name} PARTITION OF #{stream_schema()}.messages FOR VALUES IN ('#{stream.id}');
    """)
  end

  def delete_stream(%Stream{} = stream) do
    Repo.delete(stream)
  end

  defp drop_records_partition(%Stream{} = stream) do
    Repo.query!("""
    DROP TABLE IF EXISTS #{stream_schema()}.messages_#{stream.name};
    """)
  end

  # Messages

  defp messages_query(stream_id, params) do
    Enum.reduce(params, Message.where_stream_id(stream_id), fn
      {:seq_gt, seq}, query ->
        Message.where_seq_gt(query, seq)

      {:limit, limit}, query ->
        limit(query, ^limit)

      {:select, select}, query ->
        select(query, ^select)

      {:order_by, order_by}, query ->
        order_by(query, ^order_by)

      {:key_pattern, pattern}, query ->
        Message.where_key_pattern(query, pattern)

      {:keys, keys}, query ->
        Message.where_key_in(query, keys)
    end)
  end

  def list_messages_for_stream(stream_id, params \\ []) do
    stream_id
    |> messages_query(params)
    |> Repo.all()
  end

  def get_message_for_stream(stream_id, key) do
    res =
      key
      |> Message.where_key_and_stream_id(stream_id)
      |> Repo.one()

    case res do
      nil -> {:error, Error.not_found(entity: :message)}
      message -> {:ok, message}
    end
  end

  def get_message_for_stream!(stream_id, key) do
    case get_message_for_stream(stream_id, key) do
      {:ok, message} -> message
      {:error, _} -> raise Error.not_found(entity: :message)
    end
  end

  def count_messages_for_stream(stream_id, params \\ []) do
    stream_id
    |> messages_query(params)
    |> Repo.aggregate(:count, :key)
  end

  @fast_count_threshold 50_000
  def fast_count_threshold, do: @fast_count_threshold

  def fast_count_messages_for_stream(stream_id, params \\ []) do
    query = messages_query(stream_id, params)

    # This number can be pretty inaccurate
    result = Ecto.Adapters.SQL.explain(Repo, :all, query)
    [_, rows] = Regex.run(~r/rows=(\d+)/, result)

    case String.to_integer(rows) do
      count when count > @fast_count_threshold ->
        count

      _ ->
        count_messages_for_stream(stream_id, params)
    end
  end

  def approximate_storage_size_for_stream(stream_id) do
    %Stream{name: name} = Repo.get!(Stream, stream_id)

    query = """
    SELECT pg_total_relation_size('#{stream_schema()}.messages_#{name}') AS size
    """

    case Repo.query(query) do
      {:ok, %{rows: [[size]]}} -> size
      _ -> 0
    end
  end

  def upsert_messages(_stream_id, _messages) do
    # with :ok <- validate_messages(messages) do
    #   do_upsert_messages(stream_id, messages)
    # end
    :ok
  end

  # DestinationConsumer Messages

  def all_consumer_messages do
    Repo.all(ConsumerMessage)
  end

  def list_consumer_messages_for_consumer(stream_id, consumer_id, params \\ []) do
    base_query =
      consumer_id
      |> ConsumerMessage.where_consumer_id()
      |> ConsumerMessage.join_message(stream_id)
      |> ConsumerMessage.where_state_not(:acked)

    query =
      Enum.reduce(params, base_query, fn
        {:state, state}, query ->
          ConsumerMessage.where_state(query, state)

        {:state_in, states}, query ->
          ConsumerMessage.where_state_in(query, states)

        {:is_deliverable, false}, query ->
          ConsumerMessage.where_not_visible(query)

        {:is_deliverable, true}, query ->
          ConsumerMessage.where_deliverable(query)

        {:limit, limit}, query ->
          limit(query, ^limit)

        {:order_by, order_by}, query ->
          order_by(query, ^order_by)

        {:key_pattern, pattern}, query ->
          ConsumerMessage.where_key_pattern(query, pattern)
      end)

    query
    |> select([cm, m], %{consumer_message: cm, message: m})
    |> Repo.all()
    |> Enum.map(fn %{consumer_message: cm, message: m} ->
      %{cm | message: m}
    end)
  end

  def get_consumer_message(consumer_id, message_key) do
    consumer_message =
      consumer_id
      |> ConsumerMessage.where_consumer_id()
      |> ConsumerMessage.where_message_key(message_key)
      |> Repo.one()

    case consumer_message do
      nil -> {:error, Error.not_found(entity: :consumer_message)}
      consumer_message -> {:ok, consumer_message}
    end
  end

  def get_consumer_message!(consumer_id, message_key) do
    case get_consumer_message(consumer_id, message_key) do
      {:ok, consumer_message} -> consumer_message
      {:error, _} -> raise Error.not_found(entity: :consumer_message)
    end
  end

  def delete_acked_consumer_messages_for_consumer(consumer_id, limit \\ 1000) do
    subquery =
      consumer_id
      |> ConsumerMessage.where_consumer_id()
      |> ConsumerMessage.where_state(:acked)
      |> limit(^limit)

    query =
      from(cm in ConsumerMessage,
        join: acm in subquery(subquery),
        on: cm.consumer_id == acm.consumer_id and cm.message_key == acm.message_key
      )

    Repo.delete_all(query)
  end

  @spec ack_messages(Sequin.Consumers.DestinationConsumer.t(), any()) :: :ok
  def ack_messages(%DestinationConsumer{} = consumer, ack_ids) do
    Repo.transact(fn ->
      {_, _} =
        consumer.id
        |> ConsumerMessage.where_consumer_id()
        |> ConsumerMessage.where_ack_ids(ack_ids)
        |> ConsumerMessage.where_state(:pending_redelivery)
        |> Repo.update_all(set: [state: :available, not_visible_until: nil])

      if DestinationConsumer.should_delete_acked_messages?(consumer) do
        {_, _} =
          consumer.id
          |> ConsumerMessage.where_consumer_id()
          |> ConsumerMessage.where_ack_ids(ack_ids)
          |> ConsumerMessage.where_state(:delivered)
          |> Repo.delete_all()
      else
        {_, _} =
          consumer.id
          |> ConsumerMessage.where_consumer_id()
          |> ConsumerMessage.where_ack_ids(ack_ids)
          |> ConsumerMessage.where_state(:delivered)
          |> Repo.update_all(set: [state: :acked])
      end

      :ok
    end)

    :ok
  end

  @spec nack_messages(Sequin.Consumers.DestinationConsumer.t(), any()) :: :ok
  def nack_messages(%DestinationConsumer{} = consumer, ack_ids) do
    {_, _} =
      consumer.id
      |> ConsumerMessage.where_consumer_id()
      |> ConsumerMessage.where_state_not(:acked)
      |> ConsumerMessage.where_ack_ids(ack_ids)
      |> Repo.update_all(set: [state: :available, not_visible_until: nil])

    :ok
  end
end
