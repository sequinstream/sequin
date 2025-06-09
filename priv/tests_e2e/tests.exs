Code.require_file("init.exs", __DIR__)

defmodule Sequin.E2E.KafkaTest do
  use ExUnit.Case, async: false

  require Logger

  @db_config [
    hostname: "localhost",
    port: 5412,
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
    tid = :ets.new(:test_messages, [:duplicate_bag, :public])

    on_exit(fn ->
      GenServer.stop(conn)
    end)

    {:ok, %{conn: conn, tid: tid}}
  end

  setup %{conn: _conn, tid: tid} do
    :ets.delete_all_objects(tid)
    clear_kafka_topics()
    :ok
  end

  describe "sequin integration" do
    test "changes are streamed to kafka", %{conn: conn, tid: tid} do
      insert_test_data(conn, tid, @expected_count)
      messages = get_messages_until_complete(@expected_count, :timer.seconds(30))
      assert_messages(messages, @expected_count, tid)
    end

    test "changes are buffered when kafka is down and sent after recovery", %{conn: conn, tid: tid} do
      Logger.info("Stopping kafka_ex application...")
      :ok = Application.stop(:kafka_ex)
      Logger.info("kafka_ex application stopped")

      Logger.info("Stopping Kafka container...")
      :ok = docker_stop("sequin-e2e-kafka")
      Logger.info("Kafka container stopped")

      {:ok, status} = docker_status("sequin-e2e-kafka")
      assert String.trim(status) == "exited", "Kafka container should be stopped but was #{String.trim(status)}"

      insert_test_data(conn, tid, @expected_count)

      Logger.info("Starting Kafka container...")
      :ok = docker_start("sequin-e2e-kafka")
      Logger.info("Kafka container started")

      Logger.info("Waiting for Kafka to be ready...")
      wait_for_kafka_ready()
      Logger.info("Kafka is ready")

      messages = get_messages_until_complete(@expected_count, :timer.seconds(60))
      assert_messages(messages, @expected_count, tid)
    end
  end

  defp assert_messages(messages, expected_count, tid) do
    assert length(messages) >= expected_count,
           "Expected #{expected_count} messages, but got #{length(messages)}"

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

  defp wait_for_sequin(retries \\ 0) do
    case :httpc.request(:get, {~c"http://localhost:7316/health", []}, [], []) do
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

  defp clear_kafka_topics do
    for topic <- @test_topics do
      System.cmd(
        "docker",
        [
          "exec",
          "sequin-e2e-kafka",
          "kafka-topics",
          "--bootstrap-server",
          "kafka:29092",
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
          "kafka:29092",
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

  defp get_messages_until_complete(expected_count, timeout_ms, acc \\ []) do
    end_time = System.system_time(:millisecond) + timeout_ms

    if System.system_time(:millisecond) >= end_time do
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
            get_messages_until_complete(expected_count, timeout_ms, new_acc)
          end

        _ ->
          Process.sleep(1000)
          Logger.debug("Waiting for messages... (current: #{length(acc)})")
          get_messages_until_complete(expected_count, timeout_ms, acc)
      end
    end
  end

  defp wait_for_kafka_ready(retries \\ 0) do
    max_retries = 30
    retry_delay = 2000

    try do
      case Application.start(:kafka_ex) do
        :ok -> :ok
        {:error, {:already_started, _}} -> :ok
        {:error, _} -> raise "Failed to start kafka_ex application"
      end

      case KafkaEx.metadata() do
        %{brokers: [_ | _]} -> :ok
        _ -> raise "No brokers available"
      end
    catch
      _kind, _reason when retries < max_retries ->
        Logger.warning("Waiting for Kafka to be ready, retrying in #{retry_delay}ms... (#{retries + 1}/#{max_retries})")
        Process.sleep(retry_delay)
        wait_for_kafka_ready(retries + 1)

      _kind, reason ->
        Logger.error("Failed to connect to Kafka after #{max_retries} retries: #{inspect(reason)}")
        raise "Kafka not ready after #{max_retries} retries"
    end
  end

  defp docker_stop(container_id) do
    case docker_cmd(["stop", container_id]) do
      {:ok, _output} -> :ok
      :ok -> :ok
      error -> error
    end
  end

  defp docker_start(container_id) do
    case docker_cmd(["start", container_id]) do
      {:ok, _output} -> :ok
      :ok -> :ok
      error -> error
    end
  end

  defp docker_status(container_id) do
    case docker_cmd(["inspect", "--format", "{{.State.Status}}", container_id]) do
      :ok -> {:ok, "running"}
      {:ok, status} -> {:ok, String.trim(status)}
      error -> error
    end
  end

  defp docker_cmd(args) do
    case System.cmd("docker", args, stderr_to_stdout: true) do
      {output, 0} -> if output == "", do: :ok, else: {:ok, output}
      {error, _} -> {:error, error}
    end
  end
end
