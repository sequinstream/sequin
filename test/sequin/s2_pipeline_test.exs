defmodule Sequin.Runtime.S2PipelineTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.FunctionsFactory
  alias Sequin.Functions.MiniElixir
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.S2.Client

  describe "events are sent to S2" do
    setup do
      account = AccountsFactory.insert_account!()

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          type: :s2,
          batch_size: 10
        )

      {:ok, %{consumer: consumer}}
    end

    test "events are sent", %{consumer: consumer} do
      test_pid = self()
      record = CharacterFactory.character_attrs()

      event =
        [consumer_id: consumer.id]
        |> ConsumersFactory.insert_deliverable_consumer_event!()
        |> Map.put(:data, ConsumersFactory.consumer_event_data(record: record))

      Req.Test.expect(Client, fn conn ->
        assert conn.method == "POST"
        assert conn.host == "test-basin.b.aws.s2.dev"
        assert conn.request_path == "/v1/streams/#{consumer.sink.stream}/records"

        # Verify request body
        {:ok, body, _} = Plug.Conn.read_body(conn)
        body_json = Jason.decode!(body)

        # Verify records array exists and has one record
        assert %{"records" => [record]} = body_json

        # Verify record contains a "body" key
        assert %{"body" => body} = record
        assert is_binary(body), "Expected body to be a string, got: #{inspect(body)}"

        send(test_pid, {:s2_request, conn})
        Req.Test.json(conn, %{})
      end)

      start_pipeline!(consumer)

      ref = send_test_event(consumer, event)
      assert_receive {:ack, ^ref, [%{data: %ConsumerEvent{}}], []}, 1_000
      assert_receive {:s2_request, _}, 1_000
    end
  end

  describe "s2 routing" do
    setup do
      account = AccountsFactory.insert_account!()

      transform =
        FunctionsFactory.insert_function!(
          account_id: account.id,
          function_type: :transform,
          function_attrs: %{body: "record"}
        )

      routing =
        FunctionsFactory.insert_function!(
          account_id: account.id,
          function_type: :routing,
          function_attrs: %{
            body: """
              %{basin: \"other-basin\", stream: metadata.table_name}
            """
          }
        )

      {:ok, _} = MiniElixir.create(transform.id, transform.function.code)
      {:ok, _} = MiniElixir.create(routing.id, routing.function.code)

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          type: :s2,
          transform_id: transform.id,
          routing_id: routing.id,
          routing_mode: "dynamic"
        )

      {:ok, %{consumer: consumer}}
    end

    test "routing overrides basin and stream", %{consumer: consumer} do
      test_pid = self()

      record = CharacterFactory.character_attrs()

      event =
        [consumer_id: consumer.id]
        |> ConsumersFactory.insert_deliverable_consumer_event!()
        |> Map.put(:data, ConsumersFactory.consumer_event_data(record: record))

      Req.Test.expect(Client, fn conn ->
        assert conn.host == "other-basin.b.aws.s2.dev"
        assert conn.request_path == "/v1/streams/#{event.data.metadata.table_name}/records"

        send(test_pid, {:s2_request, conn})
        Req.Test.json(conn, %{})
      end)

      start_pipeline!(consumer)

      ref = send_test_event(consumer, event)
      assert_receive {:ack, ^ref, [%{data: %ConsumerEvent{}}], []}, 1_000
      assert_receive {:s2_request, _}, 1_000
    end
  end

  defp start_pipeline!(consumer) do
    start_supervised!({SinkPipeline, [consumer_id: consumer.id, producer: Broadway.DummyProducer, test_pid: self()]})
  end

  defp send_test_event(consumer, event) do
    Broadway.test_message(broadway(consumer), event, metadata: %{topic: "test", headers: []})
  end

  defp broadway(consumer) do
    SinkPipeline.via_tuple(consumer.id)
  end
end
