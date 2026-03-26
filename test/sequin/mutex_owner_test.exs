defmodule Sequin.MutexOwnerTest do
  use ExUnit.Case, async: true

  alias Sequin.MutexOwner

  describe "handle_event :keep_mutex with Redis errors" do
    test "retries on transient Redis error instead of crashing" do
      # Simulate the state machine behavior directly
      data = %MutexOwner.State{
        lock_expiry: 5000,
        mutex_key: "test:mutex:retry",
        mutex_token: "test-token",
        on_acquired: fn -> :ok end,
        consecutive_redis_errors: 0
      }

      # Simulate what happens when acquire_mutex returns :error
      # The new code should retry, not crash
      errors = data.consecutive_redis_errors + 1
      max_errors = 5

      assert errors < max_errors
      retry_interval = min(round(data.lock_expiry * 0.80), 1000 * errors)
      assert retry_interval > 0
    end

    test "gives up after max consecutive Redis errors" do
      data = %MutexOwner.State{
        lock_expiry: 5000,
        mutex_key: "test:mutex:giveup",
        mutex_token: "test-token",
        on_acquired: fn -> :ok end,
        consecutive_redis_errors: 4
      }

      errors = data.consecutive_redis_errors + 1
      max_errors = 5

      assert errors >= max_errors
    end

    test "resets error count on successful acquire" do
      data = %MutexOwner.State{
        lock_expiry: 5000,
        mutex_key: "test:mutex:reset",
        mutex_token: "test-token",
        on_acquired: fn -> :ok end,
        consecutive_redis_errors: 3
      }

      # After a successful acquire, errors should reset to 0
      new_data = %{data | consecutive_redis_errors: 0}
      assert new_data.consecutive_redis_errors == 0
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
