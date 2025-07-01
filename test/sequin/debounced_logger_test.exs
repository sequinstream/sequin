defmodule Sequin.DebouncedLoggerTest do
  use ExUnit.Case, async: true

  import ExUnit.CaptureLog

  alias Sequin.DebouncedLogger
  alias Sequin.DebouncedLogger.Config

  require Logger

  # Use very fast timers for testing
  @fast_debounce_ms 5
  @test_table :debounced_logger_test_buckets

  setup do
    # Use a separate table for tests to ensure async safety
    DebouncedLogger.setup_ets(@test_table)
    :ok
  end

  # Helper function to create configs with test defaults
  defp test_config(dedupe_key, opts \\ []) do
    test_pid = self()

    default_opts = [
      debounce_interval_ms: @fast_debounce_ms,
      table_name: @test_table,
      logger_fun: logger_fun(test_pid)
    ]

    struct!(%Config{dedupe_key: dedupe_key}, Keyword.merge(default_opts, opts))
  end

  defp logger_fun(test_pid) do
    fn level, message, metadata ->
      send(test_pid, {:log, level, message, metadata})
    end
  end

  test "logs immediately on first call" do
    config = test_config(:test_first_call)

    DebouncedLogger.warning("First message", config)

    assert_receive {:log, :warning, "First message", []}
  end

  test "suppresses subsequent calls and emits summary" do
    config = test_config(:test_suppress)

    DebouncedLogger.warning("Test message", config)
    DebouncedLogger.warning("Test message", config)
    DebouncedLogger.warning("Test message", config)

    # Should receive original message immediately
    assert_receive {:log, :warning, "Test message", []}

    # Should receive summary after timer fires
    assert_receive {:log, :warning, "[2×] Test message", []}
  end

  test "different dedupe_keys are tracked separately" do
    config1 = test_config(:key1)
    config2 = test_config(:key2)

    DebouncedLogger.warning("Message A", config1)
    DebouncedLogger.warning("Message B", config2)
    DebouncedLogger.warning("Message A", config1)

    # Should receive both messages immediately
    assert_receive {:log, :warning, "Message A", []}
    assert_receive {:log, :warning, "Message B", []}

    # Should receive summary for Message A (which was duplicated)
    assert_receive {:log, :warning, "[1×] Message A", []}
  end

  test "different log levels are tracked separately" do
    config = test_config(:same_key)

    DebouncedLogger.warning("Same message", config)
    DebouncedLogger.error("Same message", config)
    DebouncedLogger.warning("Same message", config)

    # Should receive all immediate messages
    assert_receive {:log, :warning, "Same message", []}
    assert_receive {:log, :error, "Same message", []}

    # Should receive summary for warning level (which had duplicates)
    assert_receive {:log, :warning, "[1×] Same message", []}
  end

  test "no summary emitted when only one call is made" do
    config = test_config(:single_call)

    DebouncedLogger.warning("Single message", config)

    # Should receive the immediate log
    assert_receive {:log, :warning, "Single message", []}
  end

  test "works with all log levels" do
    config = test_config(:test_levels)

    DebouncedLogger.debug("Debug msg", config)
    DebouncedLogger.info("Info msg", config)
    DebouncedLogger.warning("Warning msg", config)
    DebouncedLogger.error("Error msg", config)

    assert_receive {:log, :debug, "Debug msg", []}
    assert_receive {:log, :info, "Info msg", []}
    assert_receive {:log, :warning, "Warning msg", []}
    assert_receive {:log, :error, "Error msg", []}
  end

  test "preserves metadata" do
    config = test_config(:test_metadata)
    metadata = [request_id: "req-123"]

    DebouncedLogger.warning("Message with metadata", config, metadata)
    DebouncedLogger.warning("Message with metadata", config, metadata)

    # Should receive initial message with metadata
    assert_receive {:log, :warning, "Message with metadata", [request_id: "req-123"]}

    # Should receive summary with metadata
    assert_receive {:log, :warning, "[1×] Message with metadata", [request_id: "req-123"]}
  end

  test "merges metadata with existing logger metadata" do
    config = test_config(:test_merge_metadata)

    # Set some logger metadata
    Logger.metadata(existing_key: "existing_value")

    # Add additional metadata
    additional_metadata = [request_id: "req-456"]

    DebouncedLogger.warning("Message with merged metadata", config, additional_metadata)

    # Should receive message with both existing and additional metadata
    assert_receive {:log, :warning, "Message with merged metadata", metadata}
    assert Keyword.get(metadata, :existing_key) == "existing_value"
    assert Keyword.get(metadata, :request_id) == "req-456"
  end

  test "counts accumulate correctly with many duplicates" do
    config = test_config(:many_duplicates)

    # Send 10 identical messages
    for _ <- 1..10 do
      DebouncedLogger.warning("Repeated message", config)
    end

    # Should receive the first message immediately
    assert_receive {:log, :warning, "Repeated message", []}

    # Should receive summary with count of 9 (10 total - 1 immediate)
    assert_receive {:log, :warning, "[9×] Repeated message", []}
  end

  @tag capture_log: true
  test "uses real Logger by default" do
    # This test verifies the default behavior uses actual Logger
    # We'll create a config without specifying logger_fun
    config = %Config{
      dedupe_key: :real_logger_test,
      debounce_interval_ms: @fast_debounce_ms,
      table_name: @test_table
      # logger_fun defaults to &Logger.bare_log/3
    }

    assert capture_log(fn ->
             DebouncedLogger.warning("Real logger message", config)
           end) =~ "Real logger message"
  end
end
