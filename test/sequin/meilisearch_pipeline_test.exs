defmodule Sequin.Runtime.MeilisearchPipelineTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.CharacterFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Functions.MiniElixir
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.Meilisearch.Client

  defp send_gzipped_response(conn, status_code, response_data) do
    gzipped_body = response_data |> Jason.encode!() |> :zlib.gzip()

    conn
    |> Plug.Conn.put_resp_header("content-encoding", "gzip")
    |> Plug.Conn.send_resp(status_code, gzipped_body)
  end

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

    test "function updates are applied with routing function", %{consumer: consumer} do
      {_ids, events} = generate_events(consumer, 2)

      # Create a function that will be compiled and run by MiniElixir
      routing_function = %{
        id: Ecto.UUID.generate(),
        name: "test_routing_function",
        type: "routing",
        account_id: consumer.account_id,
        function: %{
          type: :routing,
          sink_type: :meilisearch,
          code: ~s"""
          def route(action, record, changes, metadata) do
            %{
              action: :function,
              index_name: "test",
              filter: "id = \#{record["id"]}",
              function: "doc.content[0] = context.new_content",
              context: %{new_content: "Updated content"}
            }
          end
          """
        }
      }

      assert {:ok, function} = Consumers.create_function(consumer.account_id, routing_function)
      assert MiniElixir.create(function.id, function.function.code)
      {:ok, consumer} = Consumers.update_sink_consumer(consumer, %{routing_id: function.id, routing_mode: "dynamic"})

      task_uid1 = :rand.uniform(100)
      task_uid2 = task_uid1 + 1

      # We need to handle 4 requests total: 2 POSTs and 2 GETs
      # Since we process messages individually, each POST is followed by its GET
      Req.Test.expect(Client, 4, fn conn ->
        case conn.method do
          "POST" ->
            assert conn.request_path == "/indexes/test/documents/edit"

            {:ok, body, _} = Plug.Conn.read_body(conn)
            body = Jason.decode!(body)

            assert body["filter"] =~ "id ="
            assert body["function"] == "doc.content[0] = context.new_content"
            assert body["context"]["new_content"] == "Updated content"

            # Return different task UIDs for each POST
            task_uid = if conn.assigns[:call_count] == 1, do: task_uid1, else: task_uid2
            Req.Test.json(conn, %{"taskUid" => task_uid})

          "GET" ->
            assert conn.request_path =~ "/tasks/"

            response_data = %{"status" => "succeeded"}
            send_gzipped_response(conn, 200, response_data)
        end
      end)

      start_pipeline!(consumer)
      ref = send_test_batch(consumer, events)

      assert_receive {:ack, ^ref, [_msg1, _msg2], []}, 3_000
    end

    test "function updates handle errors gracefully", %{consumer: consumer} do
      {_ids, events} = generate_events(consumer, 1)

      # Create a function that will be compiled and run by MiniElixir
      routing_function = %{
        id: Ecto.UUID.generate(),
        name: "test_routing_function_error",
        type: "routing",
        account_id: consumer.account_id,
        function: %{
          type: :routing,
          sink_type: :meilisearch,
          code: ~s"""
          def route(action, record, changes, metadata) do
            %{
              action: :function,
              index_name: "test",
              filter: "id = \#{record["id"]}",
              function: "doc.content[0] = context.new_content",
              context: %{new_content: "Updated content"}
            }
          end
          """
        }
      }

      assert {:ok, function} = Consumers.create_function(consumer.account_id, routing_function)
      assert MiniElixir.create(function.id, function.function.code)
      {:ok, consumer} = Consumers.update_sink_consumer(consumer, %{routing_id: function.id, routing_mode: "dynamic"})

      task_uid = :rand.uniform(100)

      Req.Test.expect(Client, fn conn ->
        assert conn.method == "POST"
        assert conn.request_path == "/indexes/test/documents/edit"

        Req.Test.json(conn, %{"taskUid" => task_uid})
      end)

      Req.Test.expect(Client, fn conn ->
        assert conn.method == "GET"
        assert conn.request_path == "/tasks/#{task_uid}"

        response_data = %{
          "status" => "failed",
          "error" => %{
            "message" => "Invalid function expression",
            "code" => "invalid_document_function"
          }
        }

        send_gzipped_response(conn, 200, response_data)
      end)

      start_pipeline!(consumer)
      ref = send_test_batch(consumer, events)

      assert_receive {:ack, ^ref, [], [_failed]}, 5_000
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
      assert conn.method == "POST"
      assert conn.request_path == "/indexes/test/documents"

      body_records =
        body
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

      response_data = %{
        "status" => "succeeded"
      }

      send_gzipped_response(conn, 200, response_data)
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
