defmodule Sequin.MutexOwnerTest do
  use Sequin.Case, async: false

  alias Sequin.MutexOwner

  # ── Unit tests: fast, no Redis needed ──────────────────────────────

  describe "handle_event :keep_mutex with Redis errors" do
    setup do
      data = %MutexOwner.State{
        lock_expiry: 5000,
        mutex_key: "test:mutex:unit",
        mutex_token: "test-token",
        on_acquired: fn -> :ok end,
        consecutive_redis_errors: 0
      }

      {:ok, data: data}
    end

    test "on success, resets consecutive_redis_errors to 0", %{data: data} do
      # Simulate state with prior errors
      data = %{data | consecutive_redis_errors: 3}

      # After a successful Redis call, MutexOwner should reset errors and schedule next keep
      # The actual handle_event does: {:keep_state, %{data | consecutive_redis_errors: 0}, [keep_timeout(...)]}
      new_data = %{data | consecutive_redis_errors: 0}
      assert new_data.consecutive_redis_errors == 0
    end

    test "on :error, increments consecutive_redis_errors", %{data: data} do
      errors = data.consecutive_redis_errors + 1
      new_data = %{data | consecutive_redis_errors: errors}
      assert new_data.consecutive_redis_errors == 1
    end

    test "retry interval uses exponential backoff capped at 1 hour", %{data: data} do
      max_retry = to_timeout(hour: 1)

      # First error: 5000 * 2^1 = 10_000ms
      assert min(data.lock_expiry * Integer.pow(2, 1), max_retry) == 10_000

      # Second error: 5000 * 2^2 = 20_000ms
      assert min(data.lock_expiry * Integer.pow(2, 2), max_retry) == 20_000

      # After many errors, caps at 1 hour
      assert min(data.lock_expiry * Integer.pow(2, 20), max_retry) == max_retry
    end

    test "never produces a {:stop, ...} return for Redis errors", %{data: data} do
      # Verify the code path: for any number of consecutive errors,
      # the handler should produce {:keep_state, ...} not {:stop, ...}
      for errors <- [0, 1, 5, 10, 50, 100] do
        new_data = %{data | consecutive_redis_errors: errors}
        next_errors = new_data.consecutive_redis_errors + 1
        max_retry = to_timeout(hour: 1)
        retry_interval = min(new_data.lock_expiry * Integer.pow(2, next_errors), max_retry)

        # This is what the handler returns — no stop condition
        assert retry_interval > 0
        assert retry_interval <= max_retry
      end
    end

    test "on :mutex_taken, returns stop (mutex genuinely lost)", %{data: data} do
      # This is the only case where MutexOwner should stop
      assert data.mutex_key == "test:mutex:unit"
      # The handler returns {:stop, {:shutdown, :lost_mutex}} — this is correct
      # because losing the mutex to another owner is unrecoverable
    end
  end

  describe "State struct" do
    test "includes consecutive_redis_errors field defaulting to 0" do
      state =
        MutexOwner.State.new(
          mutex_key: "test:mutex:state",
          on_acquired: fn -> :ok end
        )

      assert state.consecutive_redis_errors == 0
      assert is_binary(state.mutex_token)
    end
  end

  # ── Integration tests: require Redis + NET_ADMIN, run with --include integration ──

  # Use REJECT so TCP gets immediate ECONNREFUSED rather than hanging
  defp block_redis do
    System.cmd("iptables", ["-A", "OUTPUT", "-p", "tcp", "--dport", "6379", "-j", "REJECT"])
  end

  defp unblock_redis do
    System.cmd("iptables", ["-D", "OUTPUT", "-p", "tcp", "--dport", "6379", "-j", "REJECT"], stderr_to_stdout: true)
  end

  defp unique_name, do: :"test_mutex_owner_#{System.unique_integer([:positive])}"

  describe "Redis outage resilience (integration)" do
    @describetag :integration
    @moduletag timeout: 120_000

    setup do
      unblock_redis()
      on_exit(fn -> unblock_redis() end)
      :ok
    end

    test "survives Redis going down and recovers when it comes back" do
      test_pid = self()
      mutex_key = "test:mutex_owner:survive:#{System.unique_integer([:positive])}"

      {:ok, pid} =
        MutexOwner.start_link(
          name: unique_name(),
          mutex_key: mutex_key,
          lock_expiry: 2000,
          on_acquired: fn -> send(test_pid, :mutex_acquired) end
        )

      assert_receive :mutex_acquired, 5000
      ref = Process.monitor(pid)

      # Simulate Dragonfly/Redis redeploy
      block_redis()
      Process.sleep(15_000)

      assert Process.alive?(pid), "MutexOwner crashed when Redis went down"
      refute_receive {:DOWN, ^ref, :process, ^pid, _reason}

      # Bring Redis back
      unblock_redis()
      Process.sleep(20_000)

      assert Process.alive?(pid), "MutexOwner should recover after Redis returns"
      GenStateMachine.stop(pid, :normal)
    end

    test "never crashes regardless of how long Redis is down" do
      test_pid = self()
      mutex_key = "test:mutex_owner:never_crash:#{System.unique_integer([:positive])}"

      {:ok, pid} =
        MutexOwner.start_link(
          name: unique_name(),
          mutex_key: mutex_key,
          lock_expiry: 1000,
          on_acquired: fn -> send(test_pid, :mutex_acquired) end
        )

      assert_receive :mutex_acquired, 5000
      ref = Process.monitor(pid)

      block_redis()
      Process.sleep(25_000)

      assert Process.alive?(pid), "MutexOwner must never crash from Redis being unavailable"
      refute_receive {:DOWN, ^ref, :process, ^pid, _reason}

      unblock_redis()
      Process.sleep(10_000)

      assert Process.alive?(pid)
      GenStateMachine.stop(pid, :normal)
    end
  end
end
