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
    struct!(%Config{dedupe_key: dedupe_key, debounce_interval_ms: @fast_debounce_ms, table_name: @test_table}, opts)
  end

  test "logs immediately on first call" do
    config = test_config(:test_first_call)

    log_output =
      capture_log(fn ->
        DebouncedLogger.warning("First message", config)
      end)

    assert log_output =~ "First message"
  end

  test "suppresses subsequent calls and emits summary" do
    config = test_config(:test_suppress)

    log_output =
      capture_log(fn ->
        DebouncedLogger.warning("Test message", config)
        DebouncedLogger.warning("Test message", config)
        DebouncedLogger.warning("Test message", config)

        # Wait for timer to fire
        Process.sleep(@fast_debounce_ms + 5)
      end)

    # Should see original message and summary
    assert log_output =~ "Test message"
    assert log_output =~ "[2×] Test message"
  end

  test "different dedupe_keys are tracked separately" do
    config1 = test_config(:key1)
    config2 = test_config(:key2)

    log_output =
      capture_log(fn ->
        DebouncedLogger.warning("Message A", config1)
        DebouncedLogger.warning("Message B", config2)
        DebouncedLogger.warning("Message A", config1)

        Process.sleep(@fast_debounce_ms + 5)
      end)

    assert log_output =~ "Message A"
    assert log_output =~ "Message B"
    assert log_output =~ "[1×] Message A"
    # Message B should only appear once (no duplicates)
    refute log_output =~ "[1×] Message B"
  end

  test "different log levels are tracked separately" do
    config = test_config(:same_key)

    log_output =
      capture_log(fn ->
        DebouncedLogger.warning("Same message", config)
        DebouncedLogger.error("Same message", config)
        DebouncedLogger.warning("Same message", config)

        Process.sleep(@fast_debounce_ms + 5)
      end)

    # Should see both immediate logs and warning summary
    # Warning appears: immediate + summary = 2 times
    # Error appears: immediate only = 1 time
    # Total: 3 occurrences of "Same message"
    message_count = log_output |> String.split("Same message") |> length() |> Kernel.-(1)
    assert message_count == 3

    # Should contain the summary for warning level
    assert log_output =~ "[1×] Same message"
  end

  test "no summary emitted when only one call is made" do
    config = test_config(:single_call)

    log_output =
      capture_log(fn ->
        DebouncedLogger.warning("Single message", config)
        Process.sleep(@fast_debounce_ms + 5)
      end)

    # Should only see the immediate log, no summary
    message_count = log_output |> String.split("Single message") |> length() |> Kernel.-(1)
    assert message_count == 1
    refute log_output =~ "×"
  end

  test "works with all log levels" do
    config = test_config(:test_levels)

    log_output =
      capture_log(fn ->
        DebouncedLogger.warning("Warning msg", config)
        DebouncedLogger.error("Error msg", config)
      end)

    assert log_output =~ "Warning msg"
    assert log_output =~ "Error msg"
  end

  test "preserves metadata" do
    config = test_config(:test_metadata)
    metadata = [request_id: "req-123"]

    log_output =
      capture_log([metadata: [:request_id]], fn ->
        DebouncedLogger.warning("Message with metadata", config, metadata)
        DebouncedLogger.warning("Message with metadata", config, metadata)

        Process.sleep(@fast_debounce_ms + 5)
      end)

    # Both initial and summary logs should have metadata
    logs_with_metadata = log_output |> String.split("request_id=req-123") |> length() |> Kernel.-(1)
    assert logs_with_metadata == 2
  end
end
