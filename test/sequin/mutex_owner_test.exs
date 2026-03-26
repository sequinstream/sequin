defmodule Sequin.MutexOwnerTest do
  use Sequin.Case, async: false

  alias Sequin.MutexOwner

  @moduletag timeout: 120_000

  # Use REJECT so TCP gets immediate ECONNREFUSED
  defp block_redis do
    System.cmd("iptables", ["-A", "OUTPUT", "-p", "tcp", "--dport", "6379", "-j", "REJECT"])
  end

  defp unblock_redis do
    System.cmd("iptables", ["-D", "OUTPUT", "-p", "tcp", "--dport", "6379", "-j", "REJECT"], stderr_to_stdout: true)
  end

  defp unique_name, do: :"test_mutex_owner_#{System.unique_integer([:positive])}"

  setup do
    unblock_redis()
    on_exit(fn -> unblock_redis() end)
    :ok
  end

  describe "Redis outage resilience" do
    @tag :integration
    test "survives Redis going down and does not crash" do
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
      assert Process.alive?(pid)

      ref = Process.monitor(pid)

      # Simulate Dragonfly/Redis restart
      block_redis()

      # Wait long enough for multiple keep_mutex timeout cycles
      Process.sleep(15_000)

      # MutexOwner must NOT have crashed
      assert Process.alive?(pid), "MutexOwner crashed when Redis went down — should retry indefinitely"
      refute_receive {:DOWN, ^ref, :process, ^pid, _reason}

      # Bring Redis back and verify recovery
      unblock_redis()

      # Wait for eredis to reconnect and MutexOwner to successfully touch the mutex.
      # eredis reconnect_sleep is 5s by default, plus the current backoff interval.
      Process.sleep(20_000)

      assert Process.alive?(pid), "MutexOwner should still be alive after Redis recovers"

      GenStateMachine.stop(pid, :normal)
    end

    @tag :integration
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

      # Wait a long time — this must never crash
      Process.sleep(25_000)

      assert Process.alive?(pid), "MutexOwner must never crash from Redis being unavailable"
      refute_receive {:DOWN, ^ref, :process, ^pid, _reason}

      unblock_redis()
      Process.sleep(10_000)

      assert Process.alive?(pid)
      GenStateMachine.stop(pid, :normal)
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
end
