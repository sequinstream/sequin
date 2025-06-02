Mix.install([
  {:postgrex, "~> 0.17.5"},
  {:jason, "~> 1.4"},
  {:kafka_ex, "~> 0.13.0"}
])

Application.put_env(:kafka_ex, :brokers, [{"localhost", 9092}])
Application.put_env(:kafka_ex, :consumer_group, "e2e_test_group")
Application.put_env(:kafka_ex, :use_ssl, false)
Application.put_env(:kafka_ex, :kafka_version, "0.10.0")

{:ok, _} = Application.ensure_all_started(:postgrex)
{:ok, _} = Application.ensure_all_started(:kafka_ex)

ExUnit.start()

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

  @expected_count String.to_integer(System.get_env("TEST_MESSAGES_COUNT", "1000"))

  setup do
    Process.sleep(5_000)

    # clear_test_data()
    clear_kafka_topics()
  end

  describe "sequin integration" do
    test "changes are streamed to kafka" do
      insert_test_data(@expected_count)

      end_time = System.system_time(:second) + 30
      messages = get_messages_until_complete(@expected_count, end_time)

      assert length(messages) >= @expected_count,
        "Expected #{@expected_count} messages, but got #{length(messages)}"

      messages
      |> Enum.reduce(0, fn message, acc ->
        decoded_value = Jason.decode!(message.value)
        assert decoded_value["action"] == "insert"
        record = decoded_value["record"]
        assert is_map(record), "record should be a map"
        assert is_binary(record["demo_text"]), "demo_text should be a string"
        assert is_integer(record["id"]), "id should be a number"

        acc + 1
      end)
    end
  end

  # Internal functions

  defp pg_conn, do: Postgrex.start_link(@db_config)

  defp clear_test_data do
    {:ok, pid} = pg_conn()
    Postgrex.query(pid, "DELETE FROM demo_table", [])
    GenServer.stop(pid)
  end

  defp clear_kafka_topics do
    for topic <- @test_topics do
      System.cmd("docker", [
        "exec", "sequin-e2e-kafka",
        "kafka-topics", "--bootstrap-server", "localhost:9092",
        "--delete", "--topic", topic
      ], stderr_to_stdout: true)

      System.cmd("docker", [
        "exec", "sequin-e2e-kafka",
        "kafka-topics", "--bootstrap-server", "localhost:9092",
        "--create", "--topic", topic, "--partitions", "1", "--replication-factor", "1"
      ], stderr_to_stdout: true)
    end

    Process.sleep(1_000)
  end

  defp insert_test_data(expected_count) do
    {:ok, pid} = pg_conn()

    for n <- 0..(expected_count - 1) do
      {:ok, _} = Postgrex.query(pid, "INSERT INTO demo_table (demo_text) VALUES ($1)", ["Generated text entry ##{n}"])
    end

    GenServer.stop(pid)
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
