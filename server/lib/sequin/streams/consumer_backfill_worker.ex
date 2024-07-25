defmodule Sequin.Streams.ConsumerBackfillWorker do
  @moduledoc false
  use Oban.Worker

  alias Sequin.Streams
  alias Sequin.Streams.Consumer

  require Logger

  @limit Streams.backfill_limit()

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
      case Streams.backfill_messages_for_consumer(consumer, seq) do
        {:ok, messages} when length(messages) < @limit ->
          Logger.info("[ConsumerBackfillWorker] Upserted #{length(messages)} messages for consumer #{consumer.id}")

          if is_nil(consumer.backfill_completed_at) do
            Logger.info("[ConsumerBackfillWorker] Marking consumer #{consumer.id} as backfilled")
            {:ok, _} = Streams.update_consumer_with_lifecycle(consumer, %{backfill_completed_at: DateTime.utc_now()})
          end

          next_seq = messages |> Enum.map(& &1.seq) |> Enum.max(fn -> seq end)
          create(consumer.id, next_seq, 10)

        {:ok, messages} ->
          Logger.info("[ConsumerBackfillWorker] Upserted #{length(messages)} messages for consumer #{consumer.id}")
          next_seq = Enum.max_by(messages, fn message -> message.seq end).seq
          create(consumer.id, next_seq)
      end
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
