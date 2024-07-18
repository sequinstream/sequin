defmodule Sequin.Streams.ConsumerBackfillWorker do
  @moduledoc false
  use Oban.Worker

  alias Sequin.Streams
  alias Sequin.Streams.Consumer
  alias Sequin.Streams.ConsumerMessage

  require Logger

  @limit 10_000

  def create(consumer_id, seq \\ 0, schedule_in_seconds \\ 0)

  def create(consumer_id, seq, schedule_in_seconds) do
    %{consumer_id: consumer_id, seq: seq}
    |> new(schedule_in: schedule_in_seconds)
    |> Oban.insert()
  end

  @impl Oban.Worker
  def perform(%Oban.Job{args: %{"consumer_id" => consumer_id, "seq" => seq}}) do
    consumer = Streams.get_consumer!(consumer_id)

    if Consumer.should_delete_acked_messages?(consumer) do
      delete_acked_messages(consumer)
    else
      backfill_messages(consumer, seq)
    end
  end

  def backfill_messages(consumer, seq) do
    messages =
      Streams.list_messages_for_stream(consumer.stream_id,
        seq_gt: seq,
        limit: @limit,
        order_by: [asc: :seq],
        select: [:subject, :seq]
      )

    {:ok, _} =
      messages
      |> Enum.filter(fn message ->
        Sequin.Subject.matches?(consumer.filter_subject_pattern, message.subject)
      end)
      |> Enum.map(fn message ->
        %ConsumerMessage{
          consumer_id: consumer.id,
          message_subject: message.subject,
          message_seq: message.seq
        }
      end)
      |> Streams.upsert_consumer_messages()

    Logger.info("[ConsumerBackfillWorker] Upserted #{length(messages)} messages for consumer #{consumer.id}")

    case messages do
      messages when length(messages) < @limit ->
        if is_nil(consumer.backfill_completed_at) do
          {:ok, _} = Streams.update_consumer_with_lifecycle(consumer, %{backfill_completed_at: DateTime.utc_now()})
        end

        next_seq = messages |> Enum.map(& &1.seq) |> Enum.max(fn -> seq end)
        create(consumer.id, next_seq, 10)

      _ ->
        next_seq = Enum.max_by(messages, fn message -> message.seq end).seq
        create(consumer.id, next_seq)
    end
  end

  defp delete_acked_messages(consumer) do
    case Streams.delete_acked_consumer_messages_for_consumer(consumer.id, @limit) do
      {0, nil} ->
        :ok

      {count, nil} ->
        Logger.info("[ConsumerBackfillWorker] Deleted #{count} acked messages for consumer #{consumer.id}")
        create(consumer.id, nil)
    end
  end
end
