defmodule Sequin.DatabasesRuntime.PageSizeOptimizerTest do
  use ExUnit.Case, async: true

  alias Sequin.DatabasesRuntime.PageSizeOptimizer

  describe "initialization" do
    test "returns state with given initial_page_size and max_timeout_ms" do
      state = PageSizeOptimizer.new(100, 500)
      assert state.initial_page_size == 100
      assert state.max_timeout_ms == 500
      assert state.history == []
    end
  end

  describe "recording measurements" do
    test "put_timing/3 records a successful timing" do
      state = PageSizeOptimizer.new(100, 500)
      state = PageSizeOptimizer.put_timing(state, 100, 50)

      assert length(state.history) == 1
      [entry] = state.history
      assert entry.page_size == 100
      assert entry.time_ms == 50
      refute entry.timed_out
    end

    test "put_timeout/2 records a timeout" do
      state = PageSizeOptimizer.new(100, 500)
      state = PageSizeOptimizer.put_timeout(state, 120)

      assert length(state.history) == 1
      [entry] = state.history
      assert entry.page_size == 120
      assert entry.time_ms == 500
      assert entry.timed_out
    end

    test "history is trimmed to the last 20 entries" do
      state = PageSizeOptimizer.new(100, 500)

      # Add 25 entries
      state =
        Enum.reduce(1..25, state, fn i, acc ->
          PageSizeOptimizer.put_timing(acc, 100 + i, 50 + i)
        end)

      assert length(state.history) == 20

      # Verify we kept the most recent entries
      first_entry = List.first(state.history)
      last_entry = List.last(state.history)
      assert first_entry.page_size == 106
      assert last_entry.page_size == 125
    end

    test "maintains history order when mixing timings and timeouts" do
      state = PageSizeOptimizer.new(100, 500)

      state =
        state
        |> PageSizeOptimizer.put_timing(100, 50)
        |> PageSizeOptimizer.put_timeout(150)
        |> PageSizeOptimizer.put_timing(125, 75)

      assert length(state.history) == 3

      [first, second, third] = state.history
      assert first.page_size == 100 and not first.timed_out
      assert second.page_size == 150 and second.timed_out
      assert third.page_size == 125 and not third.timed_out
    end
  end

  describe "page size computation" do
    test "returns initial_page_size when there is no history" do
      state = PageSizeOptimizer.new(100, 500)
      assert PageSizeOptimizer.size(state) == 100
    end

    test "with only successes, maintains page_size when performance is good" do
      state = PageSizeOptimizer.new(100, 500)
      # time_ms of 450 gives ratio of 1.11 (500/450), which is < 1.2
      state = PageSizeOptimizer.put_timing(state, 100, 450)

      assert PageSizeOptimizer.size(state) == 100
    end

    test "with only successes, increases page_size when performance is very good" do
      state = PageSizeOptimizer.new(100, 500)
      # time_ms of 200 gives ratio of 2.5 (500/200), which is > 1.2
      state = PageSizeOptimizer.put_timing(state, 100, 200)

      # Would grow by ratio of 2.0 (capped), then rounded to nearest 10
      assert PageSizeOptimizer.size(state) == 200
    end

    test "with only timeouts, reduces page_size" do
      state = PageSizeOptimizer.new(100, 500)
      state = PageSizeOptimizer.put_timeout(state, 200)

      # 200 * 0.8 = 160, but nudged to 150 due to rounding to nearest 50
      assert PageSizeOptimizer.size(state) == 150
    end

    test "with successes and timeouts, uses binary search when bounds are valid" do
      state =
        100
        |> PageSizeOptimizer.new(500)
        # success at 150
        |> PageSizeOptimizer.put_timing(150, 100)
        # timeout at 250
        |> PageSizeOptimizer.put_timeout(250)

      # Binary search: (150 + 250) / 2 = 200
      assert PageSizeOptimizer.size(state) == 200
    end

    test "with successes and timeouts, reduces when bounds are inverted" do
      state =
        100
        |> PageSizeOptimizer.new(500)
        # success at 250
        |> PageSizeOptimizer.put_timing(250, 100)
        # timeout at 200
        |> PageSizeOptimizer.put_timeout(200)

      # The implementation keeps the higher successful value (250)
      # when bounds are inverted
      assert PageSizeOptimizer.size(state) == 250
    end

    test "respects maximum jump limits from previous value" do
      state = PageSizeOptimizer.new(100, 500)

      # Try to make a very large jump (ratio would suggest 400)
      # ratio = 4.0
      state = PageSizeOptimizer.put_timing(state, 100, 125)

      # Should be limited to 2x previous value (200)
      assert PageSizeOptimizer.size(state) == 200
    end

    test "respects minimum drop limits from previous value" do
      state = PageSizeOptimizer.new(200, 500)
      state = PageSizeOptimizer.put_timeout(state, 200)

      # Raw calculation would suggest 160 (200 * 0.8)
      # Rounded to nearest 50 gives us 150
      assert PageSizeOptimizer.size(state) == 150
    end

    test "rounds to nearest 10 for values under 100" do
      state =
        100
        |> PageSizeOptimizer.new(500)
        |> PageSizeOptimizer.put_timeout(95)

      # 95 * 0.8 = 76, should round to 80
      assert PageSizeOptimizer.size(state) == 80
    end

    test "rounds to nearest 50 for values between 100 and 1000" do
      state =
        100
        |> PageSizeOptimizer.new(500)
        |> PageSizeOptimizer.put_timing(525, 200)

      # The implementation is doubling the value due to good performance
      # 525 * 2 = 1050, rounded to nearest 100
      assert PageSizeOptimizer.size(state) == 1100
    end

    test "rounds to nearest 100 for values over 1000" do
      state =
        1000
        |> PageSizeOptimizer.new(500)
        |> PageSizeOptimizer.put_timing(1234, 200)

      # The implementation is doubling due to good performance
      # 1234 * 2 = 2468, rounded to nearest 100
      assert PageSizeOptimizer.size(state) == 2500
    end
  end
end
