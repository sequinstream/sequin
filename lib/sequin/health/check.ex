defmodule Sequin.Health.Check do
  @moduledoc false

  use TypedStruct

  alias __MODULE__
  alias Sequin.Error
  alias Sequin.JSON

  typedstruct do
    field :id, String.t(), enforce: true
    field :name, String.t(), enforce: true
    field :status, Sequin.Health.status(), enforce: true
    field :error, Error.t() | nil
  end

  @spec from_json!(String.t()) :: t()
  def from_json!(json) when is_binary(json) do
    json
    |> Jason.decode!()
    |> from_json()
  end

  @spec from_json(map()) :: t()
  def from_json(json) when is_map(json) do
    json
    |> JSON.decode_atom("id")
    |> JSON.decode_atom("status")
    |> JSON.decode_polymorphic("error")
    |> JSON.struct(Check)
  end

  defimpl Jason.Encoder do
    def encode(check, opts) do
      check
      |> Map.from_struct()
      |> JSON.encode_polymorphic(:error)
      |> Jason.Encode.map(opts)
    end
  end
end
