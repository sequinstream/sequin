defmodule Sequin.Factory.HealthFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Factory
  alias Sequin.Factory.ErrorFactory
  alias Sequin.Health
  alias Sequin.Health.Check
  alias Sequin.Health.HealthSnapshot
  alias Sequin.Repo

  def status, do: Factory.one_of([:healthy, :warn, :error])

  def health(attrs \\ []) do
    attrs = Map.new(attrs)

    {status, attrs} = Map.pop(attrs, :status, status())

    merge_attributes(
      %Health{
        status: status(),
        checks: [check(status: status)],
        last_healthy_at: maybe_last_healthy_at(status),
        erroring_since: maybe_erroring_since(status)
      },
      attrs
    )
  end

  def check(attrs \\ []) do
    attrs = Map.new(attrs)

    {status, attrs} = Map.pop(attrs, :status, status())

    merge_attributes(
      %Check{
        slug: Factory.atom(),
        status: status,
        error: maybe_error(status),
        initial_event_at: Factory.utc_datetime(),
        last_healthy_at: maybe_last_healthy_at(status),
        erroring_since: maybe_erroring_since(status)
      },
      attrs
    )
  end

  defp maybe_error(:error), do: ErrorFactory.random_error()
  defp maybe_error(_), do: nil

  defp maybe_last_healthy_at(:healthy), do: Factory.utc_datetime_usec()
  defp maybe_last_healthy_at(_), do: Enum.random([nil, Factory.utc_datetime_usec()])

  defp maybe_erroring_since(:error), do: Factory.utc_datetime_usec()
  defp maybe_erroring_since(_), do: nil

  def health_snapshot(attrs \\ []) do
    attrs = Map.new(attrs)

    {entity_kind, attrs} =
      Map.pop_lazy(attrs, :entity_kind, fn ->
        Enum.random([:http_endpoint, :sink_consumer, :postgres_replication_slot, :wal_pipeline])
      end)

    {status, attrs} =
      Map.pop_lazy(attrs, :status, fn ->
        Enum.random([:healthy, :warning, :error, :initializing, :waiting, :paused])
      end)

    {health_json, attrs} =
      Map.pop_lazy(attrs, :health_json, fn ->
        %{
          "checks" => [
            %{
              "slug" => "#{Factory.word()}_check",
              "status" => to_string(status),
              "error" => if(status == :error, do: ErrorFactory.random_error())
            }
          ],
          "status" => to_string(status)
        }
      end)

    merge_attributes(
      %HealthSnapshot{
        id: Factory.uuid(),
        entity_id: Factory.uuid(),
        entity_kind: entity_kind,
        status: status,
        health_json: health_json,
        sampled_at: Factory.utc_datetime_usec()
      },
      attrs
    )
  end

  def insert_health_snapshot!(attrs \\ []) do
    attrs
    |> health_snapshot()
    |> Repo.insert!()
  end
end
