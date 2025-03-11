defmodule Sequin.Bench do
  @moduledoc """
  Sequin.Bench is a benchmarking library for Elixir, inspired by Benchee.

  It allows you to easily benchmark different scenarios with various inputs and
  parallelization levels. The library takes care of warmup, timing, and statistical
  analysis of the results.

  ## Usage

  To use Sequin.Bench, you need to:

  1. Define your scenarios as a list of tuples, where each tuple contains a scenario
     name and a function to benchmark.
  2. Optionally, define inputs to test your scenarios with different data sizes.
  3. Call `Sequin.Bench.run/2` with your scenarios and options.

  ## Example

  ```elixir
  # Run the benchmark
  Sequin.Bench.run(
    [
      {"flat_map", fn [input, batch_size] -> Enum.flat_map(input, &List.duplicate(&1, batch_size)) end},
      {"map.flatten", fn [input, batch_size] -> input |> Enum.map(&List.duplicate(&1, batch_size)) |> List.flatten() end}
    ],
    max_time_ms: 3000,
    warmup_time_ms: 1000,
    parallel: [1, 4],
    inputs: [
      {"Small_Batch_2", [Enum.to_list(1..1_000), 2]},
      {"Medium_Batch_5", [Enum.to_list(1..10_000), 5]},
      {"Large_Batch_10", [Enum.to_list(1..100_000), 10]}
    ]
  )
  ```

  This will benchmark two different ways of mapping over a list and flattening the result,
  with three different input sizes and batch sizes, and two levels of parallelization.

  ## Options

  - `:max_time_ms` - Maximum time in milliseconds to run each scenario (default: 10000)
  - `:warmup_time_ms` - Time in milliseconds to warm up before starting measurements (default: 1000)
  - `:parallel` - List of parallelization levels to test (default: [1])
  - `:inputs` - List of inputs to test, each as a tuple of {name, input_args} where input_args is a list (default: [{"Default", []}])

  ## Output

  Sequin.Bench will print the results to the console and also save them to a CSV file.
  The CSV filename will be printed at the end of the benchmark run.
  """
  alias Sequin.Bench.StatsTracker

  require Logger

  def run(scenarios, options) do
    scenarios =
      Enum.map(
        scenarios,
        fn
          {name, fun} -> {name, fun, []}
          {name, fun, opts} -> {name, fun, opts}
        end
      )

    max_time_ms = Keyword.get(options, :max_time_ms, 10_000)
    warmup_time_ms = Keyword.get(options, :warmup_time_ms, 1000)
    parallel_list = Keyword.get(options, :parallel, [1])
    inputs = Keyword.get(options, :inputs, [{"Default", []}])
    out_dir = Keyword.get(options, :out_dir, File.cwd!())
    print_results = Keyword.get(options, :print_results, true)

    csv_filename = "benchmark_results_#{DateTime.to_iso8601(DateTime.utc_now())}.csv"
    csv_path = Path.join(out_dir, csv_filename)
    File.write!(csv_path, csv_header())

    results =
      for {scenario_name, scenario_fun, scenario_opts} <- scenarios,
          {input_name, input_args} <- inputs,
          parallel <- parallel_list do
        if before_fun = scenario_opts[:before] do
          Logger.info("Prepare scenario with before function.")
          before_fun.(input_args)
          Logger.info("Prepared.")
        end

        result =
          run_scenario(
            scenario_name,
            scenario_fun,
            input_name,
            input_args,
            parallel,
            max_time_ms,
            warmup_time_ms,
            print_results
          )

        with {:ok, result} <- result do
          append_to_csv(csv_path, result)
          result
        end
      end

    Logger.info("\nBenchmark results have been written to: #{csv_path}")

    results
  end

  defp run_scenario(
         scenario_name,
         scenario_fun,
         input_name,
         input_args,
         parallel,
         max_time_ms,
         warmup_time_ms,
         print_results
       ) do
    Logger.info("Starting scenario: #{scenario_name} (Input: #{input_name}, Parallel: #{parallel})")
    start_time = System.monotonic_time(:millisecond)

    {:ok, _pid} = StatsTracker.start_link([])

    tasks =
      Enum.map(1..parallel, fn _ ->
        Task.Supervisor.async_nolink(Sequin.TaskSupervisor, fn ->
          run_scenario_task(scenario_fun, input_args, max_time_ms, warmup_time_ms)
        end)
      end)

    Task.await_many(tasks, :infinity)

    flush_messages()

    case StatsTracker.get_stats(StatsTracker) do
      {:ok, stats} ->
        end_time = System.monotonic_time(:millisecond)
        total_time = end_time - start_time

        formatted_stats = format_stats("#{scenario_name} (#{input_name}, Parallel: #{parallel})", stats)

        if print_results do
          IO.puts(formatted_stats)
        end

        Logger.info(
          "Finished scenario: #{scenario_name} (Input: #{input_name}, Parallel: #{parallel}). Total time: #{format_time(total_time)}"
        )

        {:ok,
         %{
           scenario_name: scenario_name,
           input_name: input_name,
           parallel: parallel,
           stats: stats,
           total_time: total_time
         }}

      error ->
        Logger.error("Error calculating stats: #{inspect(error)}")
        :error
    end
  after
    StatsTracker.stop(StatsTracker)
  end

  defp run_scenario_task(scenario_fun, input_args, max_time_ms, warmup_time_ms) do
    start_time = System.monotonic_time(:millisecond)
    end_time = start_time + max_time_ms + warmup_time_ms

    run_loop(scenario_fun, input_args, start_time, end_time, warmup_time_ms)
  end

  defp run_loop(scenario_fun, input_args, start_time, end_time, warmup_time_ms) do
    current_time = System.monotonic_time(:millisecond)

    cond do
      current_time >= end_time ->
        :ok

      current_time - start_time <= warmup_time_ms ->
        exec_fun(scenario_fun, input_args)
        run_loop(scenario_fun, input_args, start_time, end_time, warmup_time_ms)

      true ->
        execution_time_ms = exec_fun(scenario_fun, input_args)

        if StatsTracker.add_datapoint(StatsTracker, execution_time_ms) do
          :ok
        else
          run_loop(scenario_fun, input_args, start_time, end_time, warmup_time_ms)
        end
    end
  end

  defp exec_fun(scenario_fun, input_args) do
    {execution_time, _result} = :timer.tc(fn -> apply(scenario_fun, input_args) end, :millisecond)
    execution_time
  end

  defp format_stats(test_name, stats) do
    ops_per_second = calculate_ops_per_second(stats)

    """
    ┌────────────────────────────────────────────────────────────────────────┐
    │ Test Results: #{String.pad_trailing(test_name, 56)} │
    ├────────────────────────────────────────────────────────────────────────┤
    │ Total Operations: #{String.pad_leading(to_string(stats.count), 52)} │
    │ Average Time:     #{String.pad_leading(format_time(stats.avg), 52)} │
    │ Minimum Time:     #{String.pad_leading(format_time(stats.min), 52)} │
    │ Maximum Time:     #{String.pad_leading(format_time(stats.max), 52)} │
    │ 95th Percentile:  #{String.pad_leading(format_time(stats.percentile_95), 52)} │
    │ Standard Dev:     #{String.pad_leading(format_time(stats.std_dev), 52)} │
    │ Ops/Second:       #{String.pad_leading(ops_per_second, 52)} │
    │ Means Stabilized: #{String.pad_leading(to_string(stats.mean_stabilized?), 52)} │
    └────────────────────────────────────────────────────────────────────────┘
    """
  end

  defp format_time(time_ms) when is_float(time_ms) do
    cond do
      time_ms >= 1000 ->
        "#{Float.round(time_ms / 1000, 2)} s"

      time_ms >= 1 ->
        "#{Float.round(time_ms, 2)} ms"

      true ->
        "#{Float.round(time_ms * 1000, 2)} μs"
    end
  end

  defp format_time(time_ms), do: format_time(time_ms / 1.0)

  defp calculate_ops_per_second(stats) do
    if stats.avg > 0 do
      format_float(1000 / stats.avg)
    else
      "n/a"
    end
  end

  defp format_float(float) when is_float(float) do
    float |> Float.round(2) |> Float.to_string()
  end

  defp csv_header do
    "Test Name,Input Name,Parallelization,Total Operations,Average Time (ms),Minimum Time (ms),Maximum Time (ms),95th Percentile (ms),Standard Deviation (ms),Ops/Second,Means Stabilized\n"
  end

  defp append_to_csv(filename, result) do
    line =
      "#{result.scenario_name},#{result.input_name},#{result.parallel},#{result.stats.count},#{result.stats.avg},#{result.stats.min},#{result.stats.max},#{result.stats.percentile_95},#{result.stats.std_dev},#{calculate_ops_per_second(result.stats)},#{result.stats.mean_stabilized?}\n"

    File.write!(filename, line, [:append])
  end

  def flush_messages do
    receive do
      {:DOWN, _ref, :process, _pid, _reason} ->
        flush_messages()
    after
      0 ->
        :ok
    end
  end
end
