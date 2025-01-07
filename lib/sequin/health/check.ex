defmodule Sequin.Health.Check do
  @moduledoc false

  use TypedStruct

  alias __MODULE__
  alias Sequin.Error
  alias Sequin.JSON

  @type status :: :healthy | :unhealthy | :waiting | :initializing | :stale

  typedstruct do
    field :slug, atom(), enforce: true
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

  def to_external(%Check{} = check) do
    %{
      name: check_name(check),
      status: if(check.status == :waiting, do: :initializing, else: check.status),
      error: if(check.error, do: %{message: Exception.message(check.error)})
    }
  end

  def check_name(%Check{} = check) do
    case check.slug do
      # Postgres replication slot checks
      :reachable -> "Database reachable"
      :replication_configuration -> "Valid slot"
      :replication_connected -> "Replication connected"
      :replication_messages -> "Replication messages"
      # Sink consumer checks
      :sink_configuration -> "Configuration"
      :messages_filtered -> "Messages filtered"
      :messages_ingested -> "Messages ingested"
      :messages_pending_delivery -> "Messages pending delivery"
      :messages_delivered -> "Messages delivered"
      # HTTP endpoint checks
      :endpoint_reachable -> "Endpoint reachable"
      # WAL pipeline checks
      :destination_insert -> "Destination insert"
    end
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
