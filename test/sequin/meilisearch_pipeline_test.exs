defmodule Sequin.Runtime.MeilisearchPipelineTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Functions.MiniElixir
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.Meilisearch.Client

  describe "meilisearch pipeline" do
    setup do
      account = AccountsFactory.insert_account!()

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          type: :meilisearch,
          message_kind: :record,
          sink: %{
            primary_key: "id",
            index_name: "test"
          },
          batch_size: 10
        )

      {:ok, %{consumer: consumer}}
    end

    test "events are sent (no transform)", %{consumer: consumer} do
      {ids, events} = generate_events(consumer, 2)

      verify_network_request(consumer, events, fn message, i ->
        message["id"] == Enum.at(ids, i)
      end)
    end

    test "events are sent (nested pk)", %{consumer: consumer} do
      {ids, events} = generate_events(consumer, 2)
      consumer = consumer_with_primary_key(consumer, "record.id")

      verify_network_request(consumer, events, fn message, i ->
        message["record"]["id"] == Enum.at(ids, i)
      end)
    end

    test "events are sent (simple transform)", %{consumer: consumer} do
      {ids, events} = generate_events(consumer, 2)
      consumer = consumer_with_transform(consumer, "simple", "record")

      verify_network_request(consumer, events, fn message, i ->
        message["id"] == Enum.at(ids, i)
      end)
    end

    test "events are sent (transform + nested pk)", %{consumer: consumer} do
      {ids, events} = generate_events(consumer, 2)
      consumer = consumer_with_primary_key(consumer, "test.id")

      consumer =
        consumer_with_transform(
          consumer,
          "nested-transform",
          ~s|(%{"test"=>%{"id"=>Map.get(record,"id")}})|
        )

      verify_network_request(consumer, events, fn record, i ->
        record["test"]["id"] == Enum.at(ids, i)
      end)
    end
  end

  defp generate_events(consumer, count) do
    records =
      for _ <- 1..count do
        CharacterFactory.character_attrs()
      end

    ids = Enum.map(records, &Map.get(&1, "id"))

    events =
      for record <- records do
        [consumer_id: consumer.id, source_record: :character]
        |> ConsumersFactory.insert_deliverable_consumer_record!()
        |> Map.put(:data, ConsumersFactory.consumer_record_data(record: record, action: :insert))
      end

    {
      ids,
      events
    }
  end

  defp consumer_with_primary_key(consumer, primary_key) do
    {:ok, consumer} =
      Consumers.update_sink_consumer(consumer, %{
        primary_key: primary_key
      })

    consumer
  end

  defp consumer_with_transform(consumer, name, code) do
    transform_code = """
    def transform(action, record, changes, metadata) do
     #{code}
    end
    """

    assert {:ok, transform} =
             Consumers.create_function(consumer.account_id, %{
               name: name,
               function: %{
                 type: "transform",
                 code: transform_code
               }
             })

    assert MiniElixir.create(transform.id, transform.function.code)
    {:ok, consumer} = Consumers.update_sink_consumer(consumer, %{transform_id: transform.id})
    consumer
  end

  defp verify_network_request(consumer, events, predicate) do
    test_pid = self()
    task_uid = :rand.uniform(100)

    Req.Test.expect(Client, fn conn ->
      {:ok, body, _} = Plug.Conn.read_body(conn)
      decompressed = :zlib.gunzip(body)
      assert conn.method == "POST"
      assert conn.request_path == "/indexes/test/documents"

      body_records =
        decompressed
        |> String.split("\n", trim: true)
        |> Enum.map(&Jason.decode!/1)

      assert length(body_records) == length(events)

      for i <- 0..(length(events) - 1) do
        assert predicate.(Enum.at(body_records, i), i)
      end

      send(test_pid, {:meilisearch_request, conn})

      Req.Test.json(conn, %{
        "taskUid" => task_uid
      })
    end)

    Req.Test.expect(Client, fn conn ->
      assert conn.method == "GET"
      assert conn.request_path == "/tasks/#{task_uid}"

      Req.Test.json(conn, %{
        "status" => "success"
      })
    end)

    start_pipeline!(consumer)
    ref = send_test_batch(consumer, events)
    assert_receive {:meilisearch_request, _sink}, 3_000
    assert_receive {:ack, ^ref, [_message1, _message2], []}, 3_000
  end

  defp start_pipeline!(consumer) do
    start_supervised!({SinkPipeline, [consumer_id: consumer.id, producer: Broadway.DummyProducer, test_pid: self()]})
  end

  defp send_test_batch(consumer, events) do
    Broadway.test_batch(broadway(consumer), events)
  end

  defp broadway(consumer) do
    SinkPipeline.via_tuple(consumer.id)
  end
end
