defmodule Sequin.ProcessMetrics do
  @moduledoc """
  A module for tracking and logging process metrics, including function invocations,
  runtime, and process stats.

  This module provides:
  - Automatic interval-based logging of process stats (memory, message queue length)
  - A decorator for tracking function invocations and runtime
  - Calculation of "unaccounted time" in the process
  - Percentage breakdown of time spent in each tracked function
  - Throughput tracking (count per second)
  - Gauge value tracking

  ## Usage

  1. Add ProcessMetrics to your GenServer:

  ```elixir
  defmodule MyServer do
    use GenServer
    use Sequin.ProcessMetrics,
      interval: :timer.seconds(30),
      metric_prefix: "my_app.my_server",
      tags: %{component: "my_component"},
      on_log: {__MODULE__, :process_metrics_on_log, []}
  end
  ```

  2. Use the decorator to track function metrics:

  ```elixir
  use Sequin.ProcessMetrics.Decorator

  @decorate track_metrics("process_message")
  def process_message(message) do
    # Your code here
  end
  ```

  3. Track throughput:

  ```elixir
  # Track messages processed
  Sequin.ProcessMetrics.increment_throughput("messages_processed")

  # Track messages processed with a specific count
  Sequin.ProcessMetrics.increment_throughput("messages_processed", 5)
  ```

  4. Track gauge values:

  ```elixir
  # Set a gauge value
  Sequin.ProcessMetrics.gauge("queue_size", queue_length)
  ```

  5. Optionally add dynamic metadata tags:

  ```elixir
  # Add user-specific tags
  Sequin.ProcessMetrics.metadata(%{user_id: "123"})
  ```

  ## How It Works

  - The module automatically schedules a periodic `:process_logging` message
  - When received, it collects metrics from the process dictionary and logs them
  - The decorator tracks function invocations and runtime in the process dictionary
  - StatsD metrics are sent with the configured prefix and tags
  """
  require Logger

  @metadata_key :__process_metrics_metadata__
  @metrics_key :__process_metrics__
  @metrics_last_logged_at_key :__process_metrics_last_logged_at__
  @stack_key :__process_metrics_stack__

  defmodule Metrics do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :interval_ms, :non_neg_integer
      field :busy_percent, number()
      field :memory_mb, number()
      field :message_queue_len, :non_neg_integer
      field :timing, %{atom => %{count: :non_neg_integer, total_ms: :non_neg_integer, percent: number()}}
      field :throughput, %{atom => :non_neg_integer}
      field :gauge, %{atom => :non_neg_integer}
      field :metadata, %{atom => any()}
    end
  end

  # ──────────────────────────────────────────────────────────────────────────
  # Per‑process call‑stack for exclusive‑time bookkeeping
  # Each frame is %{name: binary, start_us: integer, child_us: integer}
  # ──────────────────────────────────────────────────────────────────────────

  def __push_frame__(name, start_us) when is_binary(name) do
    frame = %{name: name, start_us: start_us, child_us: 0}
    Process.put(@stack_key, [frame | Process.get(@stack_key, [])])
    :ok
  end

  # Pops the top frame, returns {exclusive_us, inclusive_us}
  def __pop_frame__(finish_us) when is_integer(finish_us) do
    [[%{start_us: start_us, child_us: child_us} = frame | rest]] =
      [Process.get(@stack_key, [])]

    inclusive_us = finish_us - start_us
    exclusive_us = max(inclusive_us - child_us, 0)

    # propagate my inclusive time to the parent (if any)
    case rest do
      [] ->
        Process.delete(@stack_key)

      [parent | tail] ->
        parent = Map.update!(parent, :child_us, &(&1 + inclusive_us))
        Process.put(@stack_key, [parent | tail])
    end

    {exclusive_us, inclusive_us, frame.name, length(rest)}
  end

  # Default empty metrics structure
  @default_state %{
    timing: %{},
    throughput: %{},
    gauge: %{}
  }

  @doc """
  A macro that adds the ProcessMetrics behavior to a module.

  ## Options

  * `:interval` - The interval in milliseconds between logging process metrics (default: 30 seconds)
  * `:metric_prefix` - The prefix to use for StatsD metrics (default: derived from module name)
  * `:tags` - Additional tags to include in StatsD metrics (default: %{})
  * `:on_log` - A tuple of {module, function, args} to be called with metrics struct (default: nil)
  """
  defmacro __using__(opts) do
    quote bind_quoted: [opts: opts] do
      @opts opts

      defp process_metrics_interval do
        Keyword.get(@opts, :interval, :timer.seconds(10))
      end

      defp process_metrics_logger_prefix do
        Keyword.get_lazy(@opts, :logger_prefix, &default_logger_prefix/0)
      end

      defp process_metrics_metric_prefix do
        Keyword.get_lazy(@opts, :metric_prefix, &default_metric_prefix/0)
      end

      defp process_metrics_on_log do
        Keyword.get(@opts, :on_log)
      end

      defp default_logger_prefix do
        name = __MODULE__ |> Module.split() |> List.last()
        "[#{name}]"
      end

      defp default_metric_prefix do
        __MODULE__ |> to_string() |> String.downcase() |> String.replace(".", "_")
      end

      defp process_metrics_tags do
        Keyword.get(@opts, :tags, %{})
      end

      def handle_info(:start, state) do
        Process.send_after(self(), :process_logging, 0)
        Sequin.ProcessMetrics.no_reply(state)
      end

      # Add the handle_info callback for process_logging
      def handle_info(:process_logging, state) do
        handle_process_logging()

        Process.send_after(self(), :process_logging, process_metrics_interval())
        Sequin.ProcessMetrics.no_reply(state)
      end

      defp handle_process_logging do
        # Get dynamic tags from process dictionary
        dynamic_tags = Sequin.ProcessMetrics.get_metadata()
        # Merge static and dynamic tags
        tags = Map.merge(process_metrics_tags(), dynamic_tags)

        Sequin.ProcessMetrics.handle_process_logging(
          logger_prefix: process_metrics_logger_prefix(),
          tags: tags,
          on_log: process_metrics_on_log()
        )
      end
    end
  end

  # Adds compatibility with GenStage processes
  # GenStage processes use a special noreply format
  def no_reply(state) do
    {:dictionary, dictionary} = Process.info(self(), :dictionary)

    case Keyword.fetch!(dictionary, :"$initial_call") do
      {GenStage, :init, 1} ->
        {:noreply, [], state}

      _ ->
        {:noreply, state}
    end
  end

  def start do
    Process.send_after(self(), :start, 0)
  end

  # Helper functions to get and update metrics
  defp get_metrics do
    Process.get(@metrics_key, @default_state)
  end

  defp put_metrics(metrics) do
    Process.put(@metrics_key, metrics)
  end

  # Generic update function for the entire metrics map
  defp update_metrics(key, fun) when is_atom(key) and is_function(fun, 1) do
    metrics = get_metrics()
    updated_metrics = Map.update!(metrics, key, fun)
    put_metrics(updated_metrics)
    updated_metrics
  end

  # Update a specific metric within a category
  defp update_metric(category, name, default, fun) when is_atom(category) and is_binary(name) and is_function(fun, 1) do
    update_metrics(category, fn category_map ->
      Map.update(category_map, name, default, fun)
    end)
  end

  @doc """
  Sets metadata tags for metrics in the process dictionary.

  ## Parameters

  * `tags` - A map of tags to include in StatsD metrics
  """
  def metadata(tags) when is_map(tags) do
    Process.put(@metadata_key, Map.merge(Process.get(@metadata_key, %{}), tags))
  end

  @doc """
  Gets the current metadata tags from the process dictionary.

  ## Returns

  A map of tags
  """
  def get_metadata do
    Process.get(@metadata_key, %{})
  end

  @doc """
  Increments a throughput counter.

  This function tracks counts that will be converted to per-second rates
  when metrics are logged.

  ## Parameters

  * `name` - The name of the throughput metric
  * `count` - The amount to increment the counter by (default: 1)
  """
  def increment_throughput(name, count \\ 1) do
    update_metric(:throughput, name, count, &(&1 + count))
  end

  @doc """
  Sets a gauge value.

  This function stores the current value of a metric that represents
  a point-in-time measurement (like queue size, connection count, etc.)

  ## Parameters

  * `name` - The name of the gauge metric
  * `value` - The current value of the gauge
  """
  def gauge(name, value) do
    update_metric(:gauge, name, value, fn _ -> value end)
  end

  @doc """
  Updates timing metrics for a function.

  * `name` – function name
  * `time_ms` – **exclusive** runtime in milliseconds
  """
  def update_timing(name, time_ms) do
    update_metric(:timing, name, %{count: 1, total_ms: time_ms}, fn metrics ->
      %{
        count: metrics.count + 1,
        total_ms: metrics.total_ms + time_ms
      }
    end)
  end

  @doc """
  Handles the `:process_logging` tick.
  """
  def handle_process_logging(logger_prefix: logger_prefix, tags: tags, on_log: on_log) do
    now = System.monotonic_time(:millisecond)
    last_logged_at = Process.get(@metrics_last_logged_at_key)
    interval_ms = if last_logged_at, do: now - last_logged_at
    has_interval = is_integer(interval_ms) and interval_ms > 0

    info =
      Process.info(self(), [
        # Total memory used by process in bytes
        :memory,
        # Number of messages in queue
        :message_queue_len
      ])

    # Get all metrics from the process dictionary
    metrics = get_metrics()

    unaccounted_ms =
      if has_interval do
        accounted_ms = Enum.sum_by(metrics.timing, fn {_name, %{total_ms: total_ms}} -> total_ms end)
        max(0, interval_ms - accounted_ms)
      end

    busy_percent = if has_interval, do: Float.round(100 - unaccounted_ms / interval_ms * 100, 2)

    # Calculate percentages for each operation
    timing_with_percentages =
      if has_interval do
        Map.new(metrics.timing, fn {name, data} ->
          percent = Float.round(data.total_ms / interval_ms * 100, 2)
          {String.to_atom(name), Map.put(data, :percent, percent)}
        end)
      else
        %{}
      end

    timing_with_percentages =
      if has_interval do
        unaccounted_percent = Float.round(unaccounted_ms / interval_ms * 100, 2)

        Map.put(timing_with_percentages, :unaccounted, %{
          count: 1,
          total_ms: unaccounted_ms,
          percent: unaccounted_percent
        })
      else
        %{}
      end

    throughput_per_sec =
      if has_interval do
        Map.new(metrics.throughput, fn {name, count} ->
          {String.to_atom(name), Float.round(count / (interval_ms / 1000), 2)}
        end)
      else
        %{}
      end

    gauge =
      if has_interval do
        Map.new(metrics.gauge, fn {name, value} -> {String.to_atom(name), value} end)
      else
        %{}
      end

    # Create metrics struct for callback
    metrics = %Metrics{
      interval_ms: interval_ms,
      busy_percent: busy_percent,
      memory_mb: Float.round(info[:memory] / 1_024 / 1_024, 2),
      message_queue_len: info[:message_queue_len],
      timing: timing_with_percentages,
      throughput: throughput_per_sec,
      gauge: gauge,
      metadata: Map.merge(get_metadata(), tags)
    }

    # Call the on_log callback if configured
    if on_log do
      {module, function, args} = on_log
      apply(module, function, [metrics | args])
    end

    case metrics do
      %{busy_percent: nil} ->
        Logger.info("#{logger_prefix} Process metrics", logger_metadata(metrics))

      %{busy_percent: busy_percent} when busy_percent < 20 ->
        Logger.info("#{logger_prefix} Process metrics (#{busy_percent}% busy)", logger_metadata(metrics))

      %{busy_percent: busy_percent} ->
        Logger.warning("#{logger_prefix} Process metrics (#{busy_percent}% busy)", logger_metadata(metrics))
    end

    # Clear metrics after logging
    put_metrics(@default_state)

    # Schedule next logging and update last logged time
    Process.put(@metrics_last_logged_at_key, now)
  end

  defp logger_metadata(%Metrics{} = metrics) do
    [
      interval_ms: metrics.interval_ms,
      busy_percent: metrics.busy_percent,
      memory_mb: metrics.memory_mb,
      message_queue_len: metrics.message_queue_len
    ]
    |> Keyword.merge(Keyword.new(metrics.metadata))
    |> Keyword.merge(Keyword.new(metrics.gauge))
    |> Keyword.merge(Keyword.new(metrics.throughput))
    |> Keyword.merge(flatten_timing(metrics.timing))
  end

  defp flatten_timing(timing) do
    timing
    |> Enum.flat_map(fn {name, %{count: count, total_ms: total_ms, percent: percent}} ->
      [
        {String.to_atom("#{name}_total_ms"), total_ms},
        {String.to_atom("#{name}_count"), count},
        {String.to_atom("#{name}_percent"), percent}
      ]
    end)
    |> Keyword.new()
  end
end

defmodule Sequin.ProcessMetrics.Decorator do
  @moduledoc """
  Decorators for tracking **exclusive** function runtimes.
  """

  use Decorator.Define, track_metrics: 1

  @doc """
  `@decorate track_metrics("my_fun")`

  Records *self time* only; parent time is not double‑counted.
  """
  def track_metrics(name, body, _context) do
    quote do
      start_us = System.monotonic_time(:microsecond)
      # push a new frame onto the per‑process stack
      Sequin.ProcessMetrics.__push_frame__(unquote(name), start_us)

      try do
        unquote(body)
      after
        finish_us = System.monotonic_time(:microsecond)

        {exclusive_us, _inclusive_us, _fun_name, _depth} =
          Sequin.ProcessMetrics.__pop_frame__(finish_us)

        # Convert µs → ms (integer division) and record
        Sequin.ProcessMetrics.update_timing(
          unquote(name),
          div(exclusive_us, 1_000)
        )
      end
    end
  end
end
