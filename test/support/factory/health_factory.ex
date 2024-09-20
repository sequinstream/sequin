defmodule Sequin.Factory.HealthFactory do
  @moduledoc false
  import Sequin.Factory.Support

  alias Sequin.Factory
  alias Sequin.Factory.ErrorFactory
  alias Sequin.Health
  alias Sequin.Health.Check

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
        id: Factory.uuid(),
        name: Factory.word(),
        status: status,
        error: maybe_error(status),
        created_at: Factory.utc_datetime_usec()
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
end
