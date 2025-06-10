defmodule Sequin.Metrics.EntityHealthMetrics do
  @moduledoc """
  Reports entity health metrics to Prometheus via the telemetry system.

  This module collects the current health status of various Sequin entities
  (sinks, databases, replication slots) from health snapshots and exposes
  them as Prometheus metrics.
  """

  alias Sequin.Health
  alias Sequin.Health.HealthSnapshot
  alias Sequin.Prometheus

  require Logger

  @doc """
  Called by the telemetry poller to collect and report entity health metrics.

  This function queries the latest health snapshots for all entities and
  updates the prometheus gauge accordingly.
  """
  @spec report_entity_health(atom()) :: :ok
  def report_entity_health(prom_mod \\ Prometheus) do
    # Reset the gauge to handle removed entities
    prom_mod.reset_entity_health_gauge()

    try do
      # Stream latest snapshots inside a transaction
      Health.latest_snapshots_stream(fn stream ->
        Enum.each(stream, &update_entity_gauge(prom_mod, &1))
      end)

      :ok
    rescue
      e ->
        Logger.error("Failed to update entity health metrics: #{inspect(e)}")
        :ok
    end
  end

  defp update_entity_gauge(prom_mod, %HealthSnapshot{} = snapshot) do
    # Map entity_kind from HealthSnapshot to entity_type label
    entity_type =
      case snapshot.entity_kind do
        :sink_consumer -> "sink"
        :postgres_replication_slot -> "replication_slot"
        _ -> to_string(snapshot.entity_kind)
      end

    # Map status to the expected values for the gauge
    status_values = [
      {"ok", snapshot.status == :healthy},
      {"warn", snapshot.status == :warning},
      {"error", snapshot.status in [:error]}
    ]

    # Set gauge values for each status
    Enum.each(status_values, fn {status_label, is_current_status} ->
      value = if is_current_status, do: 1, else: 0

      prom_mod.set_entity_health(
        entity_type,
        snapshot.entity_id,
        status_label,
        value
      )
    end)
  end
end
