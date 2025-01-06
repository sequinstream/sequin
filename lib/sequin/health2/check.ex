defmodule Sequin.Health2.Check do
  @moduledoc false

  use TypedStruct

  alias __MODULE__
  alias Sequin.Error
  alias Sequin.JSON

  @type status :: :healthy | :unhealthy | :waiting | :initializing | :stale

  typedstruct do
    field :slug, atom(), enforce: true
    field :name, String.t()
    field :status, status(), enforce: true
    field :error, Error.t() | nil
    field :initial_event_at, DateTime.t() | nil
    field :last_healthy_at, DateTime.t() | nil
    field :erroring_since, DateTime.t() | nil
  end

  @spec from_json!(String.t()) :: t()
  def from_json!(json) when is_binary(json) do
    json
    |> Jason.decode!()
    |> from_json!()
  end

  @spec from_json!(map()) :: t()
  def from_json!(json) when is_map(json) do
    json
    |> JSON.decode_atom("slug")
    |> JSON.decode_atom("status")
    |> JSON.decode_polymorphic("error")
    |> JSON.decode_timestamp("last_healthy_at")
    |> JSON.decode_timestamp("initial_event_at")
    |> JSON.decode_timestamp("erroring_since")
    |> Map.put_new("created_at", DateTime.utc_now())
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
