defmodule Sequin.Health.PagerdutyAlerterWorker do
  @moduledoc """
  Worker that checks health status of databases and consumers, sending alerts to PagerDuty as needed.
  """
  use Oban.Worker,
    queue: :health_checks,
    max_attempts: 1

  alias Sequin.Consumers
  alias Sequin.Health
  alias Sequin.Pagerduty
  alias Sequin.Replication
  alias Sequin.Repo

  require Logger

  @impl Oban.Worker
  def perform(_job) do
    if Pagerduty.enabled?() do
      check_replications()
      check_consumers()
    end

    :ok
  end

  defp check_replications do
    Replication.all_active_pg_replications()
    |> Repo.preload([:postgres_database, :account])
    |> Enum.each(fn replication ->
      case Health.get(replication.postgres_database) do
        {:ok, health} -> handle_database_health(replication, health)
        {:error, error} -> Logger.error("Failed to get database health: #{inspect(error)}")
      end
    end)
  end

  defp check_consumers do
    # Get all active consumers that belong to active replication slots
    active_replications = Replication.all_active_pg_replications()
    active_replication_ids = Enum.map(active_replications, & &1.id)

    Consumers.all_consumers()
    |> Enum.filter(&(&1.replication_slot_id in active_replication_ids))
    |> Enum.each(fn consumer ->
      case Health.get(consumer) do
        {:ok, health} -> handle_consumer_health(consumer, health)
        {:error, error} -> Logger.error("Failed to get consumer health: #{inspect(error)}")
      end
    end)
  end

  defp handle_database_health(replication, health) do
    dedup_key = "database_health_#{replication.postgres_database_id}"

    case health.status do
      :healthy ->
        Pagerduty.resolve(dedup_key, "Database #{replication.postgres_database.name} is healthy")

      status when status in [:error, :warning] ->
        summary = build_database_error_summary(replication, health)
        severity = if status == :error, do: :critical, else: :warning

        Pagerduty.alert(dedup_key, summary, severity: severity)

      _ ->
        :ok
    end
  end

  defp handle_consumer_health(consumer, health) do
    dedup_key = "consumer_health_#{consumer.id}"

    case health.status do
      :healthy ->
        Pagerduty.resolve(dedup_key, "Consumer #{consumer.name} is healthy")

      status when status in [:error, :warning] ->
        summary = build_consumer_error_summary(consumer, health)
        severity = if status == :error, do: :critical, else: :warning

        Pagerduty.alert(dedup_key, summary, severity: severity)

      _ ->
        :ok
    end
  end

  defp build_database_error_summary(replication, health) do
    error_checks = Enum.filter(health.checks, &(&1.status in [:error, :warning]))

    check_details =
      Enum.map_join(error_checks, "\n", fn check ->
        "- #{check.name}: #{check.status}"
      end)

    """
    Database #{replication.postgres_database.name} (account "#{replication.account.name}") is experiencing issues:
    #{check_details}
    """
  end

  defp build_consumer_error_summary(consumer, health) do
    consumer = Repo.preload(consumer, [:account])
    error_checks = Enum.filter(health.checks, &(&1.status in [:error, :warning]))

    check_details =
      Enum.map_join(error_checks, "\n", fn check ->
        "- #{check.name}: #{check.status}"
      end)

    """
    Consumer #{consumer.name} (account "#{consumer.account.name}") is experiencing issues:
    #{check_details}
    """
  end

  def enqueue do
    %{}
    |> new()
    |> Oban.insert()
  end
end
