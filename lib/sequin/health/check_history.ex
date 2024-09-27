defmodule Sequin.Health.CheckHistory do
  @moduledoc false

  use TypedStruct

  alias __MODULE__
  alias Sequin.Health.Check
  alias Sequin.JSON

  @max_history 100

  typedstruct do
    field :id, String.t(), enforce: true
    field :name, String.t(), enforce: true
    field :message, String.t(), default: nil
    field :checks, [Check.t()], default: []
    field :created_at, DateTime.t(), enforce: true
  end

  def add_check(%CheckHistory{} = history, %Check{} = check) do
    updated_checks = Enum.take([check | history.checks], @max_history)
    %{history | checks: updated_checks}
  end

  def success_rate(%CheckHistory{checks: checks}) do
    total = length(checks)
    successes = Enum.count(checks, &(&1.status == :healthy))

    case {total, successes} do
      {0, 0} -> nil
      {0, _} -> 0
      {_, _} -> round(successes / total * 100)
    end
  end

  def current_status(%CheckHistory{} = history) do
    case success_rate(history) do
      nil -> :initializing
      rate when rate > 90 -> :healthy
      rate when rate > 50 -> :warning
      _ -> :error
    end
  end

  def from_json!(json) when is_binary(json) do
    json
    |> Jason.decode!()
    |> from_json()
  end

  def from_json!(json) when is_map(json) do
    from_json(json)
  end

  def from_json(json) when is_binary(json) do
    with {:ok, json} <- Jason.decode(json), do: {:ok, from_json(json)}
  end

  def from_json(json) when is_map(json) do
    checks =
      json
      |> Map.fetch!("checks")
      |> Enum.map(&Check.from_json/1)

    json
    |> JSON.decode_atom("id")
    |> JSON.decode_timestamp("created_at")
    |> Map.put("checks", checks)
    |> JSON.struct(CheckHistory)
  end

  defimpl Jason.Encoder do
    def encode(check, opts) do
      check
      |> Map.from_struct()
      |> Jason.Encode.map(opts)
    end
  end
end
