defmodule Sequin.Streams.ConsumerBackfillWorkerTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Consumers.Consumer
  alias Sequin.Error.NotFoundError
  alias Sequin.Factory.StreamsFactory
  alias Sequin.Streams
  alias Sequin.Streams.ConsumerBackfillWorker
  alias Sequin.Streams.ConsumerMessage

  @moduletag skip: true

  describe "create/1" do
    test "creates a job with valid args" do
      consumer = StreamsFactory.insert_consumer!()
      assert {:ok, %Oban.Job{}} = ConsumerBackfillWorker.create(consumer.id)

      assert_enqueued(worker: ConsumerBackfillWorker, args: %{"consumer_id" => consumer.id, "seq" => 0})
    end
  end

  describe "perform/1" do
    setup do
      consumer = StreamsFactory.insert_consumer!(filter_key_pattern: "prefix.>", backfill_completed_at: nil)

      %{consumer: consumer}
    end

    test "backfills a message with matching filter key", %{consumer: consumer} do
      # Insert a message with matching key
      message = StreamsFactory.insert_message!(stream_id: consumer.stream_id, key: "prefix.matches")

      # Create and perform the Oban job
      perform_job_for_consumer(consumer)

      # Verify that a consumer message was created
      assert consumer_message = Streams.get_consumer_message!(consumer.id, message.key)
      assert consumer_message.message_key == message.key

      # Verify the next job was enqueued
      assert_enqueued(worker: ConsumerBackfillWorker, args: %{"consumer_id" => consumer.id})
    end

    test "does not backfill a message with non-matching filter key", %{consumer: consumer} do
      # Insert a message with non-matching key
      message = StreamsFactory.insert_message!(stream_id: consumer.stream_id, key: "non.matching.key")

      # Create and perform the Oban job
      perform_job_for_consumer(consumer)

      # Verify that no consumer message was created
      assert_raise NotFoundError, fn ->
        Streams.get_consumer_message!(consumer.id, message.key)
      end

      # Verify the next job was enqueued (even though no message was backfilled)
      assert_enqueued(worker: ConsumerBackfillWorker, args: %{"consumer_id" => consumer.id})
    end

    test "does not backfill a message in a different stream", %{consumer: consumer} do
      # Insert a message in a different stream
      other_stream = StreamsFactory.insert_stream!()
      message = StreamsFactory.insert_message!(stream_id: other_stream.id, key: "prefix.matches")

      # Create and perform the Oban job
      perform_job_for_consumer(consumer)

      # Verify that no consumer message was created
      assert_raise NotFoundError, fn ->
        Streams.get_consumer_message!(consumer.id, message.key)
      end

      # Verify a dragnet job was enqueued
      assert_enqueued(
        worker: ConsumerBackfillWorker,
        args: %{"consumer_id" => consumer.id}
      )
    end

    test "backfill updates the consumer to backfill_completed_at: now when fewer than @limit messages are backfilled", %{
      consumer: consumer
    } do
      # Create and perform the Oban job (with no messages in the stream)
      perform_job_for_consumer(consumer, 10)

      assert consumer = Consumers.get_consumer!(consumer.id)
      assert consumer.backfill_completed_at

      assert_enqueued(
        worker: ConsumerBackfillWorker,
        args: %{"consumer_id" => consumer.id}
      )
    end

    test "backfills multiple messages in a single job", %{consumer: consumer} do
      # Insert multiple messages with matching key
      messages =
        Enum.map(1..5, fn i ->
          StreamsFactory.insert_message!(stream_id: consumer.stream_id, key: "prefix.matches.#{i}")
        end)

      # Create and perform the Oban job
      perform_job_for_consumer(consumer)

      # Verify that consumer messages were created for all messages
      for message <- messages do
        assert consumer_message = Streams.get_consumer_message!(consumer.id, message.key)
        assert consumer_message.message_key == message.key
      end

      assert_enqueued(
        worker: ConsumerBackfillWorker,
        args: %{"consumer_id" => consumer.id}
      )
    end

    test "a backfill_completed consumer has acked_messages deleted", %{consumer: consumer} do
      one_day_ago = DateTime.add(DateTime.utc_now(), -24, :hour)
      assert {:ok, _} = Consumers.update_consumer_with_lifecycle(consumer, %{backfill_completed_at: one_day_ago})

      # Insert a message with matching key
      StreamsFactory.insert_message!(stream_id: consumer.stream_id, key: "prefix.matches")
      StreamsFactory.insert_consumer_message!(consumer_id: consumer.id, message_key: "prefix.matches", state: :acked)

      assert [_] = Repo.all(ConsumerMessage)

      # Create and perform the Oban job
      perform_job_for_consumer(consumer)

      assert [] = Repo.all(ConsumerMessage)

      # Verify another backfill job was enqueued
      assert_enqueued(
        worker: ConsumerBackfillWorker,
        args: %{"consumer_id" => consumer.id}
      )
    end

    test "a consumer with backfill_completed_at set does not create a new job when no messages are deleted", %{
      consumer: consumer
    } do
      one_day_ago = DateTime.add(DateTime.utc_now(), -24, :hour)
      assert {:ok, _} = Consumers.update_consumer_with_lifecycle(consumer, %{backfill_completed_at: one_day_ago})

      # Create and perform the Oban job
      perform_job_for_consumer(consumer)

      # Verify no next job was enqueued
      refute_enqueued(worker: ConsumerBackfillWorker)
    end
  end

  defp perform_job_for_consumer(%Consumer{} = consumer, seq \\ 0) do
    perform_job(ConsumerBackfillWorker, %{"consumer_id" => consumer.id, "seq" => seq})
  end
end
