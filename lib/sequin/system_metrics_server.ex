defmodule Sequin.SystemMetricsServer do
  @moduledoc false
  use GenServer

  require Logger

  @interval :timer.seconds(30)

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

    Logger.info(
      """
      [SystemMetricsServer]
      CPU Load:      #{format_cpu_load(cpu_load)}
      Memory Total:  #{format_bytes(memory_info[:total])}
        Processes:   #{format_bytes(memory_info[:processes])}
        Atoms:       #{format_bytes(memory_info[:atom])}
        Binary:      #{format_bytes(memory_info[:binary])}
        Code:        #{format_bytes(memory_info[:code])}
        ETS:         #{format_bytes(memory_info[:ets])}
      """,
      cpu_load: cpu_load,
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

  defp format_cpu_load(nil), do: "Not available"
  defp format_cpu_load(util), do: "#{Float.round(util, 2)}%"
end
