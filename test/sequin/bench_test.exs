defmodule Sequin.BenchTest do
  use ExUnit.Case

  alias Sequin.Bench

  describe "run/2" do
    setup do
      default_opts = [
        warmup_time_ms: 0,
        out_dir: System.tmp_dir!(),
        print_results: false
      ]

      [default_opts: default_opts]
    end

    test "returns correct structure for a simple scenario", %{default_opts: default_opts} do
      scenarios = [
        {"constant_time", fn sleep_time -> :timer.sleep(sleep_time) end}
      ]

      options =
        Keyword.merge(default_opts,
          max_time_ms: 100,
          parallel: [1],
          inputs: [{"test_input", [10]}]
        )

      results = Bench.run(scenarios, options)

      assert length(results) == 1
      [result] = results

      assert result.scenario_name == "constant_time"
      assert result.input_name == "test_input"
      assert result.parallel == 1
      assert is_map(result.stats)
      assert is_integer(result.total_time)

      # Check if any CSV file was created in the specified directory
      files = File.ls!(default_opts[:out_dir])
      assert Enum.any?(files, &String.ends_with?(&1, ".csv")), "No CSV file found in the output directory"
    end

    test "stats are within expected ranges for constant time function", %{default_opts: default_opts} do
      scenarios = [
        {"constant_time", fn sleep_time -> :timer.sleep(sleep_time) end}
      ]

      options =
        Keyword.merge(default_opts,
          max_time_ms: 100,
          parallel: [1],
          inputs: [{"test_input", [10]}]
        )

      [result] = Bench.run(scenarios, options)

      assert result.stats.avg >= 10 && result.stats.avg < 25, "Average time should be close to 10ms"
      assert result.stats.min >= 10, "Minimum time should be at least 10ms"
      assert result.stats.max < 20, "Maximum time should be less than 20ms"
      assert result.stats.std_dev < 2, "Standard deviation should be small for constant time function"
    end

    @tag capture_log: true
    test "executes tests in parallel", %{default_opts: default_opts} do
      test_pid = self()

      scenarios = [
        {"parallel_test",
         fn _arg ->
           unless Process.get(:sent?) do
             send(test_pid, {:run, self()})
             Process.put(:sent?, true)
           end

           :ok
         end}
      ]

      options =
        Keyword.merge(default_opts,
          max_time_ms: 50,
          parallel: [1, 4],
          inputs: [{"test_input", [1]}]
        )

      Bench.run(scenarios, options)

      received_pids =
        Enum.reduce(1..5, [], fn _, pids ->
          receive do
            {:run, pid} -> [pid | pids]
          after
            100 -> pids
          end
        end)

      assert length(Enum.uniq(received_pids)) > 1, "Expected multiple unique PIDs, got: #{inspect(received_pids)}"
    end
  end
end
