defmodule Sequin.SystemMetricsServer do
  @moduledoc false
  use GenServer

  require Logger

  @interval :timer.seconds(30)
  @run_queue_threshold 50

  def start_link(_opts) do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  def init(state) do
    # Print metrics immediately on start
    schedule_log()
    log_metrics()
    {:ok, state}
  end

  def handle_info(:log_metrics, state) do
    log_metrics()
    schedule_log()
    {:noreply, state}
  end

  defp schedule_log do
    Process.send_after(self(), :log_metrics, @interval)
  end

  defp log_metrics do
    memory_info = :erlang.memory()
    cpu_load = cpu_load()
    scheduler_util = scheduler_util()

    run_queue = :erlang.statistics(:run_queue)

    if run_queue > @run_queue_threshold do
      Logger.warning("[SystemMetricsServer] Run queue is high (#{run_queue}), system may be resource constrained")
    end

    Logger.info(
      """
      [SystemMetricsServer]
      CPU Load:         #{format_percentage(cpu_load)}
      Scheduler Util:   #{format_percentage(scheduler_util)}
      Run Queue:        #{run_queue}
      Memory Total:     #{format_bytes(memory_info[:total])}
        Processes:      #{format_bytes(memory_info[:processes])}
        Atoms:          #{format_bytes(memory_info[:atom])}
        Binary:         #{format_bytes(memory_info[:binary])}
        Code:           #{format_bytes(memory_info[:code])}
        ETS:            #{format_bytes(memory_info[:ets])}
      """,
      cpu_load: cpu_load,
      scheduler_util: scheduler_util,
      run_queue: run_queue,
      memory_total_bytes: memory_info[:total],
      memory_processes_bytes: memory_info[:processes],
      memory_atoms_bytes: memory_info[:atom],
      memory_binary_bytes: memory_info[:binary],
      memory_code_bytes: memory_info[:code],
      memory_ets_bytes: memory_info[:ets]
    )
  end

  defp format_bytes(bytes) when is_integer(bytes) do
    cond do
      bytes >= 1_000_000_000 -> "#{Float.round(bytes / 1_000_000_000, 2)} GB"
      bytes >= 1_000_000 -> "#{Float.round(bytes / 1_000_000, 2)} MB"
      bytes >= 1_000 -> "#{Float.round(bytes / 1_000, 2)} KB"
      true -> "#{bytes} B"
    end
  end

  defp cpu_load do
    case :cpu_sup.util() do
      {:error, _reason} -> nil
      util when is_float(util) -> util
    end
  end

  # Get a 3 second snapshot of the scheduler utilization
  # Weighted: Total utilization of all normal and dirty-cpu schedulers, weighted against maximum amount of available CPU time.
  defp scheduler_util do
    [
      {:total, _, _},
      {:weighted, scheduler_util, _} | _rest
    ] = :scheduler.utilization(3)

    # Convert to percentage
    scheduler_util * 100
  end

  defp format_percentage(nil), do: "Not available"
  defp format_percentage(util), do: "#{Float.round(util, 1)}%"
end
