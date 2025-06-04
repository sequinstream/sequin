Code.require_file("init.exs", __DIR__)

defmodule Sequin.E2E.KafkaTest do
  use ExUnit.Case, async: false

  require Logger

  @db_config [
    hostname: "localhost",
    port: 5432,
    username: "postgres",
    password: "postgres",
    database: "sequin_test"
  ]

  @test_topics ["demo-topic"]
  @max_retries 10
  @retry_delay 500
  @expected_count String.to_integer(System.get_env("TEST_MESSAGES_COUNT", "1000"))

  setup_all do
    wait_for_sequin()
    wait_for_kafka()

    {:ok, conn} = Postgrex.start_link(@db_config)
    on_exit(fn -> GenServer.stop(conn) end)

    {:ok, %{conn: conn}}
  end

  setup %{conn: _conn} do
    # clear_test_data(conn)
    clear_kafka_topics()
    :ok
  end

  describe "sequin integration" do
    test "changes are streamed to kafka", %{conn: conn} do
      tid = :ets.new(:test_messages, [:set, :public])
      insert_test_data(conn, tid, @expected_count)

      end_time = System.system_time(:second) + 30
      messages = get_messages_until_complete(@expected_count, end_time)

      assert length(messages) >= @expected_count,
             "Expected #{@expected_count} messages, but got #{length(messages)}"

      Enum.reduce(messages, 0, fn message, acc ->
        decoded_value = Jason.decode!(message.value)
        assert decoded_value["action"] == "insert"
        record = decoded_value["record"]
        assert [{record["demo_text"]}] == :ets.take(tid, record["demo_text"])
        assert is_map(record), "record should be a map"
        assert is_binary(record["demo_text"]), "demo_text should be a string"
        assert is_integer(record["id"]), "id should be a number"

        acc + 1
      end)

      ets_size = :ets.info(tid, :size)
      assert ets_size == 0, "Expected 0 messages in ets table, but got #{ets_size}"
    end
  end

  defp wait_for_sequin(retries \\ 0) do
    case :httpc.request(:get, {~c"http://localhost:7376/health", []}, [], []) do
      {:ok, {{_, 200, _}, _, _}} ->
        :ok

      _ when retries < @max_retries ->
        Logger.warning("Sequin not ready, retrying in #{@retry_delay}ms... (#{retries + 1}/#{@max_retries})")
        Process.sleep(@retry_delay)
        wait_for_sequin(retries + 1)

      _ ->
        raise "Sequin is not available after #{@max_retries} retries"
    end
  end

  defp wait_for_kafka(retries \\ 0) do
    case match?(%{brokers: [_ | _]}, KafkaEx.metadata()) do
      true ->
        :ok

      _ when retries < @max_retries ->
        Logger.warning("Kafka not ready, retrying in #{@retry_delay}ms... (#{retries + 1}/#{@max_retries})")
        Process.sleep(@retry_delay)
        wait_for_kafka(retries + 1)

      _ ->
        raise "Kafka is not available after #{@max_retries} retries"
    end
  end

  # Internal functions

  defp clear_test_data(conn) do
    Postgrex.query(conn, "DELETE FROM demo_table", [])
  end

  defp clear_kafka_topics do
    for topic <- @test_topics do
      System.cmd(
        "docker",
        [
          "exec",
          "sequin-e2e-kafka",
          "kafka-topics",
          "--bootstrap-server",
          "localhost:9092",
          "--delete",
          "--topic",
          topic
        ],
        stderr_to_stdout: true
      )

      System.cmd(
        "docker",
        [
          "exec",
          "sequin-e2e-kafka",
          "kafka-topics",
          "--bootstrap-server",
          "localhost:9092",
          "--create",
          "--topic",
          topic,
          "--partitions",
          "1",
          "--replication-factor",
          "1"
        ],
        stderr_to_stdout: true
      )
    end

    Process.sleep(1_000)
  end

  defp insert_test_data(conn, tid, expected_count) do
    0..(expected_count - 1)
    |> Task.async_stream(
      fn n ->
        text = "Generated text entry ##{n}"
        true = :ets.insert(tid, {text})
        {:ok, _} = Postgrex.query(conn, "INSERT INTO demo_table (demo_text) VALUES ($1)", [text])
      end,
      max_concurrency: 10
    )
    |> Stream.run()
  end

  defp get_messages_until_complete(expected_count, end_time, acc \\ []) do
    if System.system_time(:second) >= end_time do
      Logger.debug("Timeout waiting for messages. Expected: #{expected_count} messages, got: #{length(acc)}")
      flunk("Timeout waiting for messages")
    else
      case KafkaEx.fetch("demo-topic", 0, offset: length(acc)) do
        [%{partitions: [%{message_set: messages}]}] ->
          new_acc = acc ++ messages
          Logger.debug("Received #{length(messages)} new messages, total: #{length(new_acc)}")

          if length(new_acc) >= expected_count do
            new_acc
          else
            Process.sleep(1000)
            get_messages_until_complete(expected_count, end_time, new_acc)
          end

        _ ->
          Process.sleep(1000)
          Logger.debug("Waiting for messages... (current: #{length(acc)})")
          get_messages_until_complete(expected_count, end_time, acc)
      end
    end
  end
end
