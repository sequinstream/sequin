defmodule Sequin.Health.Check do
  @moduledoc false

  use TypedStruct

  alias __MODULE__
  alias Sequin.Error
  alias Sequin.JSON

  @type status :: :healthy | :error | :warning | :waiting | :initializing | :stale | :notice

  typedstruct do
    field :slug, atom(), enforce: true
    field :status, status(), enforce: true
    field :name, String.t()
    field :error, Error.t() | nil
    field :error_slug, atom() | nil
    field :initial_event_at, DateTime.t() | nil
    field :last_healthy_at, DateTime.t() | nil
    field :erroring_since, DateTime.t() | nil
    field :extra, map(), default: %{}
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
    error_code = if check.error && Map.has_key?(check.error, :code), do: check.error.code
    error_details = if check.error && Map.has_key?(check.error, :details), do: check.error.details

    %{
      slug: check.slug,
      name: check_name(check),
      status: if(check.status == :waiting, do: :initializing, else: check.status),
      error: if(check.error, do: %{message: Exception.message(check.error), code: error_code, details: error_details}),
      error_slug: check.error_slug,
      extra: check.extra
    }
  end

  def check_name(%Check{name: nil} = check) do
    case check.slug do
      # Postgres replication slot checks
      :reachable -> "Database reachability"
      :replication_configuration -> "Replication slot configuration"
      :replication_connected -> "Replication slot connection"
      :replication_messages -> "Replication slot messages"
      # Sink consumer checks
      :sink_configuration -> "Sink configuration"
      :messages_filtered -> "Message filtering"
      :messages_ingested -> "Message ingestion"
      :messages_pending_delivery -> "Messages pending delivery"
      :messages_delivered -> "Message delivery"
      # HTTP endpoint checks
      :endpoint_reachable -> "Endpoint reachability"
      # WAL pipeline checks
      :destination_insert -> "Destination insert"
      :slot_health -> "Replication slot health"
    end
  end

  def check_name(%Check{name: name}) do
    name
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
