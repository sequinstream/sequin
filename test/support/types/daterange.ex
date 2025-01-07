defmodule Sequin.TestSupport.Types.Daterange do
  @moduledoc false
  use Ecto.Type

  def type, do: :daterange

  def cast(nil), do: {:ok, nil}

  def cast([lower, upper]) do
    {:ok, [lower, upper]}
  end

  def cast(_), do: :error

  def load(nil), do: {:ok, nil}

  def load(%Postgrex.Range{lower: lower, upper: nil}) do
    lower = to_datetime(lower)
    {:ok, [lower, nil]}
  end

  def load(%Postgrex.Range{lower: lower, upper: upper}) do
    lower = to_datetime(lower)
    upper = to_datetime(upper)
    {:ok, [lower, upper]}
  end

  def dump(nil), do: {:ok, nil}

  def dump([lower, upper]) do
    {:ok, %Postgrex.Range{lower: lower, upper: upper, upper_inclusive: false}}
  end

  def dump(_), do: :error

  defp to_datetime(value) do
    case NaiveDateTime.from_iso8601(value) do
      {:ok, naive} -> DateTime.from_naive!(naive, "Etc/UTC")
      _ -> value
    end
  end
end
