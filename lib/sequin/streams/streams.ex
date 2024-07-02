defmodule Sequin.Streams do
  @moduledoc false
  import Ecto.Query

  alias Sequin.Error
  alias Sequin.Repo
  alias Sequin.Streams.Consumer
  alias Sequin.Streams.ConsumerMessage
  alias Sequin.Streams.Message
  alias Sequin.Streams.Query
  alias Sequin.Streams.Stream
  alias Sequin.StreamsRuntime

  # General

  def reload(%Message{} = msg) do
    # Repo.reload/2 does not support compound pks
    msg.subject |> Message.where_subject_and_stream_id(msg.stream_id) |> Repo.one()
  end

  def reload(%ConsumerMessage{} = cm) do
    cm.consumer_id
    |> ConsumerMessage.where_consumer_id()
    |> ConsumerMessage.where_message_subject(cm.message_subject)
    |> Repo.one()
  end

  # Streams

  def list_streams_for_account(account_id) do
    account_id |> Stream.where_account_id() |> Repo.all()
  end

  def get_stream_for_account(account_id, id) do
    res = account_id |> Stream.where_account_id() |> Stream.where_id(id) |> Repo.one()

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
    CREATE TABLE streams.messages_#{stream.slug} PARTITION OF streams.messages FOR VALUES IN ('#{stream.id}');
    """)
  end

  def delete_stream(%Stream{} = stream) do
    Repo.delete(stream)
  end

  defp drop_records_partition(%Stream{} = stream) do
    Repo.query!("""
    DROP TABLE IF EXISTS streams.messages_#{stream.slug};
    """)
  end

  # Consumers

  def all_consumers do
    Repo.all(Consumer)
  end

  def get_consumer!(consumer_id) do
    consumer_id
    |> Consumer.where_id()
    |> Repo.one!()
  end

  def list_consumers_for_account(account_id) do
    account_id |> Consumer.where_account_id() |> Repo.all()
  end

  def list_consumers_for_stream(stream_id) do
    stream_id |> Consumer.where_stream_id() |> Repo.all()
  end

  def get_consumer_for_account(account_id, id) do
    res = account_id |> Consumer.where_account_id() |> Consumer.where_id(id) |> Repo.one()

    case res do
      nil -> {:error, Error.not_found(entity: :consumer)}
      consumer -> {:ok, consumer}
    end
  end

  def create_consumer_for_account_with_lifecycle(account_id, attrs) do
    Repo.transact(fn ->
      with {:ok, consumer} <- create_consumer(account_id, attrs),
           :ok <- create_consumer_partition(consumer) do
        {:ok, consumer}
      end
    end)
  end

  def create_consumer_with_lifecycle(attrs) do
    account_id = Map.fetch!(attrs, :account_id)

    create_consumer_for_account_with_lifecycle(account_id, attrs)
  end

  def delete_consumer_with_lifecycle(consumer) do
    Repo.transact(fn ->
      case delete_consumer(consumer) do
        :ok ->
          StreamsRuntime.Supervisor.stop_for_consumer(consumer.id)
          :ok = delete_consumer_partition(consumer)
          consumer

        error ->
          error
      end
    end)
  end

  def update_consumer_with_lifecycle(%Consumer{} = consumer, attrs) do
    consumer
    |> Consumer.update_changeset(attrs)
    |> Repo.update()
  end

  def create_consumer(account_id, attrs) do
    %Consumer{account_id: account_id}
    |> Consumer.create_changeset(attrs)
    |> Repo.insert()
  end

  def delete_consumer(%Consumer{} = consumer) do
    Repo.delete(consumer)
  end

  defp create_consumer_partition(%Consumer{} = consumer) do
    consumer = Repo.preload(consumer, :stream)

    """
    CREATE TABLE streams.consumer_messages_#{consumer.stream.slug}_#{consumer.slug} PARTITION OF streams.consumer_messages FOR VALUES IN ('#{consumer.id}');
    """
    |> Repo.query()
    |> case do
      {:ok, %Postgrex.Result{command: :create_table}} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  defp delete_consumer_partition(%Consumer{} = consumer) do
    consumer = Repo.preload(consumer, :stream)

    """
    DROP TABLE IF EXISTS streams.consumer_messages_#{consumer.stream.slug}_#{consumer.slug};
    """
    |> Repo.query()
    |> case do
      {:ok, %Postgrex.Result{command: :drop_table}} -> :ok
      {:error, error} -> {:error, error}
    end
  end

  def next_for_consumer(%Consumer{} = consumer, opts \\ []) do
    batch_size = Keyword.get(opts, :batch_size, 100)
    not_visible_until = DateTime.add(DateTime.utc_now(), consumer.ack_wait_ms, :millisecond)
    now = NaiveDateTime.utc_now()
    max_ack_pending = consumer.max_ack_pending

    available_oms =
      consumer.id
      |> ConsumerMessage.where_consumer_id()
      |> order_by([cm], asc: cm.message_seq)
      |> limit(^max_ack_pending)

    available_oms_query =
      from(acm in subquery(available_oms), as: :consumer_message)
      |> ConsumerMessage.where_deliverable()
      |> order_by([acm], asc: acm.message_seq)
      |> limit(^batch_size)
      |> lock("FOR UPDATE SKIP LOCKED")
      |> select([acm], acm.message_subject)

    Repo.transact(fn ->
      case Repo.all(available_oms_query) do
        aom_subjects when aom_subjects != [] ->
          {_, messages} =
            Repo.update_all(
              from(cm in ConsumerMessage,
                where: cm.message_subject in ^aom_subjects,
                join: m in Message,
                on: m.subject == cm.message_subject and m.stream_id == ^consumer.stream_id,
                select: %{ack_id: cm.ack_id, message: m}
              ),
              set: [
                state: "delivered",
                not_visible_until: not_visible_until,
                deliver_count: dynamic([cm], cm.deliver_count + 1),
                last_delivered_at: now
              ]
            )

          messages =
            Enum.map(messages, fn %{ack_id: ack_id, message: message} ->
              %{message | ack_id: ack_id}
            end)

          {:ok, messages}

        [] ->
          {:ok, []}

        error ->
          error
      end
    end)
  end

  # Messages

  def get_message!(subject, stream_id) do
    subject
    |> Message.where_subject_and_stream_id(stream_id)
    |> Repo.one!()
  end

  def upsert_messages(stream_id, messages, is_retry? \\ false) do
    now = DateTime.utc_now()

    messages =
      Enum.map(messages, fn message ->
        message
        |> Sequin.Map.from_ecto()
        |> Message.put_tokens()
        |> Message.put_data_hash()
        |> Map.put(:updated_at, now)
        |> Map.put(:inserted_at, now)
        |> Map.put(:stream_id, stream_id)
      end)

    on_conflict =
      from(m in Message,
        where: fragment("? IS DISTINCT FROM ?", m.data_hash, fragment("EXCLUDED.data_hash")),
        update: [
          set: [
            data: fragment("EXCLUDED.data"),
            data_hash: fragment("EXCLUDED.data_hash"),
            updated_at: fragment("EXCLUDED.updated_at")
          ]
        ]
      )

    consumers = list_consumers_for_stream(stream_id)

    Repo.transact(fn ->
      {count, messages} =
        Repo.insert_all(
          Message,
          messages,
          on_conflict: on_conflict,
          conflict_target: [:subject, :stream_id],
          timeout: :timer.seconds(30),
          returning: [:subject, :stream_id, :seq]
        )

      {consumer_ids, message_subjects, message_seqs} =
        consumers
        |> Enum.flat_map(fn consumer ->
          messages
          |> Enum.filter(fn message ->
            Consumer.filter_matches_subject?(consumer.filter_subject, message.subject)
          end)
          |> Enum.map(fn message ->
            {UUID.string_to_binary!(consumer.id), message.subject, message.seq}
          end)
        end)
        |> Enum.reduce({[], [], []}, fn {consumer_id, message_subject, message_seq}, {ids, subjects, seqs} ->
          {[consumer_id | ids], [message_subject | subjects], [message_seq | seqs]}
        end)

      Query.upsert_consumer_messages(
        consumer_ids: consumer_ids,
        message_subjects: message_subjects,
        message_seqs: message_seqs
      )

      {:ok, count}
    end)
  rescue
    e in Postgrex.Error ->
      if e.postgres.code == :character_not_in_repertoire and is_retry? == false do
        messages =
          Enum.map(messages, fn %{data: data} = message ->
            Map.put(message, :data, String.replace(data, "\u0000", ""))
          end)

        upsert_messages(stream_id, messages, true)
      else
        reraise e, __STACKTRACE__
      end
  end

  # Outstanding Messages

  def all_consumer_messages do
    Repo.all(ConsumerMessage)
  end

  def get_consumer_message!(consumer_id, message_subject) do
    # Repo.get!(ConsumerMessage, consumer_id: consumer_id, message_subject: message_subject)
    consumer_id
    |> ConsumerMessage.where_consumer_id()
    |> ConsumerMessage.where_message_subject(message_subject)
    |> Repo.one!()
  end

  def ack_messages(consumer_id, ack_ids) do
    Repo.transact(fn ->
      {_, _} =
        consumer_id
        |> ConsumerMessage.where_consumer_id()
        |> ConsumerMessage.where_ack_ids(ack_ids)
        |> ConsumerMessage.where_state(:pending_redelivery)
        |> Repo.update_all(set: [state: :available, not_visible_until: nil])

      {_, _} =
        consumer_id
        |> ConsumerMessage.where_consumer_id()
        |> ConsumerMessage.where_ack_ids(ack_ids)
        |> ConsumerMessage.where_state(:delivered)
        |> Repo.delete_all()

      :ok
    end)

    :ok
  end

  def nack_messages(consumer_id, ack_ids) do
    {_, _} =
      consumer_id
      |> ConsumerMessage.where_consumer_id()
      |> ConsumerMessage.where_ack_ids(ack_ids)
      |> Repo.update_all(set: [state: :available, not_visible_until: nil])

    :ok
  end

  def list_consumer_messages_for_consumer(consumer_id) do
    consumer_id
    |> ConsumerMessage.where_consumer_id()
    |> Repo.all()
  end
end
