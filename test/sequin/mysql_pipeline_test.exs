defmodule Sequin.Runtime.MysqlPipelineTest do
  use Sequin.DataCase, async: true

  import Mox

  alias Sequin.Factory.AccountsFactory
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.FunctionsFactory
  alias Sequin.Functions.MiniElixir
  alias Sequin.Runtime.SinkPipeline
  alias Sequin.Sinks.MysqlMock

  setup :verify_on_exit!

  describe "mysql pipeline" do
    setup do
      account = AccountsFactory.insert_account!()

      transform =
        FunctionsFactory.insert_function!(
          account_id: account.id,
          function_type: :transform,
          function_attrs: %{body: "record"}
        )

      MiniElixir.create(transform.id, transform.function.code)

      consumer =
        ConsumersFactory.insert_sink_consumer!(
          account_id: account.id,
          type: :mysql,
          message_kind: :event,
          transform_id: transform.id
        )

      {:ok, %{consumer: consumer}}
    end

    test "one message is upserted", %{consumer: consumer} do
      message =
        ConsumersFactory.consumer_message(
          consumer_id: consumer.id,
          message_kind: consumer.message_kind,
          data:
            ConsumersFactory.consumer_message_data(
              message_kind: consumer.message_kind,
              action: :insert,
              record: %{"id" => 1, "name" => "test-name"}
            )
        )

      expect(MysqlMock, :upsert_records, fn sink, records ->
        assert sink.table_name == consumer.sink.table_name
        assert length(records) == 1
        assert List.first(records)["name"] == "test-name"
        :ok
      end)

      start_pipeline!(consumer)

      send_test_batch(consumer, [message])

      assert_receive {:ack, _ref, [success], []}, 3000
      assert success.data == message
    end

    test "multiple messages are batched and upserted", %{consumer: consumer} do
      messages =
        for i <- 1..3 do
          ConsumersFactory.insert_consumer_message!(
            consumer_id: consumer.id,
            message_kind: consumer.message_kind,
            data:
              ConsumersFactory.consumer_message_data(
                message_kind: consumer.message_kind,
                action: :insert,
                record: %{"id" => i, "name" => "test-name-#{i}"}
              )
          )
        end

      expect(MysqlMock, :upsert_records, fn sink, records ->
        assert sink.table_name == consumer.sink.table_name
        assert length(records) == 3
        assert Enum.map(records, & &1["name"]) == ["test-name-1", "test-name-2", "test-name-3"]
        :ok
      end)

      start_pipeline!(consumer)

      send_test_batch(consumer, messages)

      assert_receive {:ack, _ref, successful, []}, 3000
      assert length(successful) == 3
    end

    test "delete action removes records", %{consumer: consumer} do
      message =
        ConsumersFactory.consumer_message(
          consumer_id: consumer.id,
          message_kind: consumer.message_kind,
          data:
            ConsumersFactory.consumer_message_data(
              message_kind: consumer.message_kind,
              action: :delete,
              record: %{"id" => 1},
              record_pks: [1]
            )
        )

      expect(MysqlMock, :delete_records, fn sink, record_pks ->
        assert sink.table_name == consumer.sink.table_name
        assert record_pks == [1]
        :ok
      end)

      start_pipeline!(consumer)

      send_test_batch(consumer, [message])

      assert_receive {:ack, _ref, [success], []}, 3000
      assert success.data == message
    end

    test "handles mysql errors gracefully", %{consumer: consumer} do
      message =
        ConsumersFactory.consumer_message(
          consumer_id: consumer.id,
          message_kind: consumer.message_kind,
          data:
            ConsumersFactory.consumer_message_data(
              message_kind: consumer.message_kind,
              action: :insert,
              record: %{"id" => 1, "name" => "test-name"}
            )
        )

      error = Sequin.Error.service(service: :mysql, message: "Connection failed")

      expect(MysqlMock, :upsert_records, fn _sink, _records ->
        {:error, error}
      end)

      start_pipeline!(consumer)

      send_test_batch(consumer, [message])

      assert_receive {:failed, _ref, [failed], []}, 3000
      assert failed.data == message
    end

    test "dynamic routing uses correct table name", %{consumer: consumer} do
      # Update consumer to use dynamic routing
      routing =
        FunctionsFactory.insert_function!(
          account_id: consumer.account_id,
          function_type: :routing,
          function_attrs: %{body: ~s|%{table_name: "dynamic_table"}|}
        )

      MiniElixir.create(routing.id, routing.function.code)

      consumer = %{consumer | routing_id: routing.id, routing: routing}

      message =
        ConsumersFactory.consumer_message(
          consumer_id: consumer.id,
          message_kind: consumer.message_kind,
          data:
            ConsumersFactory.consumer_message_data(
              message_kind: consumer.message_kind,
              action: :insert,
              record: %{"id" => 1, "name" => "test-name"}
            )
        )

      expect(MysqlMock, :upsert_records, fn sink, records ->
        assert sink.table_name == "dynamic_table"
        assert length(records) == 1
        :ok
      end)

      start_pipeline!(consumer)

      send_test_batch(consumer, [message])

      assert_receive {:ack, _ref, [success], []}, 3000
      assert success.data == message
    end
  end

  defp start_pipeline!(consumer) do
    start_supervised!(
      {SinkPipeline,
       [
         consumer_id: consumer.id,
         producer: Broadway.DummyProducer,
         test_pid: self()
       ]}
    )
  end

  defp send_test_batch(consumer, events) do
    Broadway.test_batch(broadway(consumer), events)
  end

  defp broadway(consumer) do
    SinkPipeline.via_tuple(consumer.id)
  end
end
