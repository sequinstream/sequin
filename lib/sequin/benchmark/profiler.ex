defmodule Sequin.Benchmark.Profiler do
  @moduledoc """
  Per-stage pipeline profiling for benchmarks.

  Places entry/exit checkpoints at each pipeline stage to measure:
  - **Processing time** = exit - entry (how long the stage actively works)
  - **Queue/wait time** = entry - previous stage's exit (how long waiting before stage started)

  Per-message state is tracked in ETS keyed by `{commit_lsn, commit_idx}`.
  At each checkpoint, computes the delta from the previous checkpoint, classifies it,
  and aggregates into bounded stats (count, sum, min, max + reservoir sample for percentiles).

  All checkpoint calls are guarded by `enabled?()` using `persistent_term` for zero overhead
  when disabled.
  """

  @messages_table :benchmark_profiler_messages
  @intervals_table :benchmark_profiler_intervals
  @samples_table :benchmark_profiler_samples

  @max_samples 10_000
  # Only track 1 in N messages through the pipeline. Unsampled messages
  # skip all checkpoint/ETS work entirely (the checkpoint function no-ops
  # when the message isn't in the messages table).
  @sample_rate 1000

  # Maps {prev_checkpoint, current_checkpoint} to an interval name
  @intervals %{
    {:born, :producer_out} => :slot_producer_processing,
    {:producer_out, :processor_in} => :processor_queue,
    {:processor_in, :processor_out} => :processor_processing,
    {:processor_out, :reorder_in} => :reorder_buffer_queue,
    {:reorder_in, :reorder_out} => :reorder_buffer_processing,
    {:reorder_out, :sps_in} => :sps_queue,
    {:sps_in, :sps_out} => :sps_processing,
    {:sps_out, :sms_in} => :message_handler_processing,
    {:sms_in, :sms_out} => :sms_buffer_wait,
    # For --through sps (BenchmarkMessageHandler is final stage):
    {:sps_out, :sink_in} => :message_handler_processing,
    # For --through reorder_buffer (mock flush is final stage):
    {:reorder_out, :sink_in} => :sink_processing
  }

  # Ordered list of stages with their processing and queue interval names
  @stages [
    {:slot_producer, :slot_producer_processing, nil},
    {:processor, :processor_processing, :processor_queue},
    {:reorder_buffer, :reorder_buffer_processing, :reorder_buffer_queue},
    {:sps, :sps_processing, :sps_queue},
    {:message_handler, :message_handler_processing, nil},
    {:sms, nil, :sms_buffer_wait},
    {:sink, :sink_processing, nil}
  ]

  @stage_labels %{
    slot_producer: "SlotProducer",
    processor: "Processor (decode)",
    reorder_buffer: "ReorderBuffer",
    sps: "SlotProcessorServer",
    message_handler: "MessageHandler",
    sms: "SlotMessageStore",
    sink: "Sink"
  }

  @doc "Initialize ETS tables and enable profiling."
  def init do
    :ets.new(@messages_table, [:set, :public, :named_table, write_concurrency: true])
    :ets.new(@intervals_table, [:set, :public, :named_table, write_concurrency: true])
    :ets.new(@samples_table, [:set, :public, :named_table])

    # Initialize all known intervals
    for {_pair, interval_name} <- @intervals do
      :ets.insert(@intervals_table, {interval_name, 0, 0, nil, nil})
      :ets.insert(@samples_table, {interval_name, 0, []})
    end

    :persistent_term.put(:benchmark_profiler_enabled, true)
    :ok
  end

  @doc "Check if profiling is enabled. Uses persistent_term for fast reads."
  def enabled? do
    :persistent_term.get(:benchmark_profiler_enabled, false)
  end

  @doc "Record the birth of a message (first checkpoint). Only samples 1 in @sample_rate messages."
  def start_message(commit_lsn, commit_idx) do
    if :erlang.phash2({commit_lsn, commit_idx}, @sample_rate) == 0 do
      now = :erlang.monotonic_time(:microsecond)
      :ets.insert(@messages_table, {{commit_lsn, commit_idx}, :born, now})
    end

    :ok
  end

  @doc "Record a checkpoint for a single message."
  def checkpoint(commit_lsn, commit_idx, checkpoint_name) do
    now = :erlang.monotonic_time(:microsecond)
    key = {commit_lsn, commit_idx}

    case :ets.lookup(@messages_table, key) do
      [{^key, prev_name, prev_ts}] ->
        delta = now - prev_ts
        pair = {prev_name, checkpoint_name}

        case Map.get(@intervals, pair) do
          nil -> :ok
          interval_name -> record_interval(interval_name, delta)
        end

        :ets.insert(@messages_table, {key, checkpoint_name, now})
        :ok

      [] ->
        # Message not tracked (profiling may have started mid-flight)
        :ok
    end
  end

  @doc "Record a checkpoint for a batch of messages. Works with any struct that has commit_lsn and commit_idx fields."
  def checkpoint_batch(messages, checkpoint_name) do
    Enum.each(messages, fn msg ->
      checkpoint(msg.commit_lsn, msg.commit_idx, checkpoint_name)
    end)
  end

  @doc "Clean up per-message tracking state."
  def finalize_message(commit_lsn, commit_idx) do
    :ets.delete(@messages_table, {commit_lsn, commit_idx})
    :ok
  end

  @doc "Generate a report with percentile stats for each interval."
  def report do
    @intervals
    |> Map.values()
    |> Enum.uniq()
    |> Enum.map(fn interval_name ->
      case :ets.lookup(@intervals_table, interval_name) do
        [{^interval_name, count, sum_us, min_us, max_us}] when count > 0 ->
          samples = get_samples(interval_name)
          sorted = Enum.sort(samples)

          {interval_name,
           %{
             count: count,
             sum_us: sum_us,
             avg_us: sum_us / count,
             min_us: min_us,
             max_us: max_us,
             p50_us: percentile(sorted, 0.50),
             p95_us: percentile(sorted, 0.95),
             p99_us: percentile(sorted, 0.99)
           }}

        _ ->
          nil
      end
    end)
    |> Enum.reject(&is_nil/1)
  end

  @doc "Format and print the profiling report as a colored terminal table."
  def format_report(report_data) do
    report_map = Map.new(report_data)

    # Compute total processing time (sum of all interval averages weighted by count)
    total_sum = Enum.reduce(report_data, 0, fn {_name, stats}, acc -> acc + stats.sum_us end)

    IO.puts("")
    IO.puts("\e[36m\e[1mPipeline Profile (all times in ms):\e[0m")

    IO.puts(
      "  \e[1m#{pad_right("Stage", 26)} | #{pad_right("Processing (p50/p95)", 22)} | #{pad_right("Queue/Wait (p50/p95)", 22)} | % Total\e[0m"
    )

    IO.puts("  #{String.duplicate("-", 26)}-+-#{String.duplicate("-", 22)}-+-#{String.duplicate("-", 22)}-+--------")

    for {stage_key, proc_interval, queue_interval} <- @stages do
      proc_stats = Map.get(report_map, proc_interval)
      queue_stats = Map.get(report_map, queue_interval)

      # Skip stages with no data
      if proc_stats || queue_stats do
        label = Map.fetch!(@stage_labels, stage_key)

        proc_str =
          if proc_stats do
            "#{format_ms(proc_stats.p50_us)}  / #{format_ms(proc_stats.p95_us)}"
          else
            "        --"
          end

        queue_str =
          if queue_stats do
            "#{format_ms(queue_stats.p50_us)}  / #{format_ms(queue_stats.p95_us)}"
          else
            "        --"
          end

        stage_sum = (if proc_stats, do: proc_stats.sum_us, else: 0) + (if queue_stats, do: queue_stats.sum_us, else: 0)

        pct =
          if total_sum > 0 do
            Float.round(stage_sum / total_sum * 100, 0) |> trunc()
          else
            0
          end

        IO.puts(
          "  #{pad_right(label, 26)} | #{pad_right(proc_str, 22)} | #{pad_right(queue_str, 22)} | #{pad_left("#{pct}%", 6)}"
        )
      end
    end

    # E2E summary
    IO.puts("  #{String.duplicate("-", 26)}-+-#{String.duplicate("-", 22)}-+-#{String.duplicate("-", 22)}-+--------")

    # Compute e2e from the first and last intervals that have data
    all_p50s = report_data |> Enum.map(fn {_name, stats} -> stats.p50_us end)
    all_p95s = report_data |> Enum.map(fn {_name, stats} -> stats.p95_us end)

    e2e_p50 = Enum.sum(all_p50s)
    e2e_p95 = Enum.sum(all_p95s)

    IO.puts(
      "  #{pad_right("End-to-end (sum)", 26)} | #{pad_right("#{format_ms(e2e_p50)}  / #{format_ms(e2e_p95)}", 47)} |   100%"
    )

    IO.puts("")

    # Print sample counts
    IO.puts("\e[36mSample counts per interval:\e[0m")

    for {name, stats} <- report_data do
      IO.puts("  #{name}: #{stats.count} samples")
    end

    IO.puts("")
  end

  @doc "Clean up ETS tables and disable profiling."
  def cleanup do
    :persistent_term.erase(:benchmark_profiler_enabled)

    for table <- [@messages_table, @intervals_table, @samples_table] do
      try do
        :ets.delete(table)
      rescue
        ArgumentError -> :ok
      end
    end

    :ok
  end

  # Private helpers

  defp record_interval(interval_name, delta_us) do
    # Update aggregate stats using :ets.update_counter for count and sum
    # For min/max, we need a different approach
    case :ets.lookup(@intervals_table, interval_name) do
      [{^interval_name, 0, 0, nil, nil}] ->
        :ets.insert(@intervals_table, {interval_name, 1, delta_us, delta_us, delta_us})

      [{^interval_name, count, sum, min_us, max_us}] ->
        new_min = min(min_us, delta_us)
        new_max = max(max_us, delta_us)
        :ets.insert(@intervals_table, {interval_name, count + 1, sum + delta_us, new_min, new_max})
    end

    add_sample(interval_name, delta_us)
  end

  defp add_sample(interval_name, delta_us) do
    case :ets.lookup(@samples_table, interval_name) do
      [{^interval_name, n, _samples}] when n >= @max_samples ->
        # At cap, skip — message-level sampling keeps total count bounded
        :ok

      [{^interval_name, n, samples}] ->
        :ets.insert(@samples_table, {interval_name, n + 1, [delta_us | samples]})

      [] ->
        :ets.insert(@samples_table, {interval_name, 1, [delta_us]})
    end
  end

  defp get_samples(interval_name) do
    case :ets.lookup(@samples_table, interval_name) do
      [{^interval_name, _n, samples}] -> samples
      [] -> []
    end
  end

  defp percentile([], _p), do: 0

  defp percentile(sorted, p) do
    k = max(0, round(p * length(sorted)) - 1)
    Enum.at(sorted, k, 0)
  end

  defp format_ms(us) when is_number(us) do
    ms = us / 1000
    :erlang.float_to_binary(ms, decimals: 1)
  end

  defp pad_right(str, len) do
    str = to_string(str)
    padding = max(0, len - String.length(str))
    str <> String.duplicate(" ", padding)
  end

  defp pad_left(str, len) do
    str = to_string(str)
    padding = max(0, len - String.length(str))
    String.duplicate(" ", padding) <> str
  end
end
