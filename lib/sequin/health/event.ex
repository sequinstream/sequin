defmodule Sequin.Health.Event do
  @moduledoc """
  Struct for health-related events. Events are unique by `slug` for a given entity ({entity_kind, entity_id}).

  We only store the latest event for a given slug in Redis, and so fold certain fields
  from the previous event into the new event.

  Events in Sequin’s health subsystem capture when a particular event (including a check/probe) last occurred,
  what its outcome was (`:success`, `:fail`, `:warning`, or `:info`), and how long it has remained
  in that outcome.

  Rather than storing a full history of every event occurrence, this module
  provides a snapshot of the **current** status, along with relevant timestamps.
  """

  use TypedStruct

  alias __MODULE__
  alias Sequin.Error
  alias Sequin.JSON

  @typedoc """
  The status of an individual event occurrence.
  """
  @statuses [:success, :fail, :warning, :info]
  @type status :: :success | :fail | :warning | :info

  @postgres_replication_slot_event_slugs [
    :db_connectivity_checked,
    :replication_slot_checked,
    :replication_connected,
    :replication_message_processed,
    :replication_heartbeat_received,
    :replication_heartbeat_verification,
    :replication_memory_limit_exceeded,
    :replication_lag_checked
  ]

  @sink_consumer_event_slugs [
    :sink_config_checked,
    :messages_filtered,
    :messages_ingested,
    :messages_pending_delivery,
    :messages_delivered,
    :alert_replica_identity_not_full_dismissed,
    :alert_toast_columns_detected_dismissed,
    :toast_columns_detected,
    :backfill_fetch_batch,
    :backfill_finished
  ]

  @http_endpoint_event_slugs [
    :endpoint_reachable
  ]

  @wal_pipeline_event_slugs [
    :messages_filtered,
    :messages_ingested,
    :messages_fetch,
    :messages_delete,
    :destination_insert
  ]

  typedstruct do
    # Short identifier or category for this event. e.g., :user_permission_probe
    field :slug, atom(), enforce: true

    # The status/kind of this particular event
    field :status, status(), default: :success

    # Optional: error details if status = :fail
    field :error, Error.t() | nil

    # When the event first occurred
    field :initial_event_at, DateTime.t()

    # When the event last occurred
    field :last_event_at, DateTime.t()

    # When the event last occurred with a successful status
    field :last_success_at, DateTime.t()

    # When the event last occurred with a failing status
    field :last_fail_at, DateTime.t()

    # How long the event has been in its current status
    field :in_status_since, DateTime.t()

    # Data payload of the event
    field :data, map(), default: %{}
  end

  def valid_status?(status), do: status in @statuses
  def valid_slug?(:postgres_replication_slot, slug), do: slug in @postgres_replication_slot_event_slugs
  def valid_slug?(:sink_consumer, slug), do: slug in @sink_consumer_event_slugs
  def valid_slug?(:http_endpoint, slug), do: slug in @http_endpoint_event_slugs
  def valid_slug?(:wal_pipeline, slug), do: slug in @wal_pipeline_event_slugs

  @doc """
  We debounce event persistence to avoid excessive writes when health events are not changing.
  For now, we just care about status changes: if the status is the same as the previous event,
  we'll debounce it for a bit.
  """
  def debounce_hash(%__MODULE__{} = event) do
    event
    |> Map.take([:status, :data])
    |> :erlang.phash2()
  end

  @doc """
  We store events keyed by slug, and only store the latest event for each slug. Therefore, to get
  helpful timestamp information, we compute timestamps for the incoming event based on timestamps
  from the existing event. This lets us propagate e.g. `in_status_since` without saving a history
  of all events.
  """
  def set_timestamps(nil, %Event{status: status} = event) do
    now = Sequin.utc_now()

    event
    |> Map.put(:initial_event_at, now)
    |> Map.put(:last_event_at, now)
    |> Map.put(:last_success_at, if(status == :success, do: now))
    |> Map.put(:last_fail_at, if(status == :fail, do: now))
    |> Map.put(:in_status_since, now)
  end

  def set_timestamps(existing_event, %Event{status: status} = event) do
    now = Sequin.utc_now()

    event
    |> Map.put(:initial_event_at, existing_event.initial_event_at)
    |> Map.put(:last_event_at, now)
    |> Map.put(
      :last_success_at,
      if status == :success do
        now
      else
        existing_event.last_success_at
      end
    )
    |> Map.put(
      :last_fail_at,
      if status == :fail do
        now
      else
        existing_event.last_fail_at
      end
    )
    |> Map.put(
      :in_status_since,
      if existing_event.status == status do
        existing_event.in_status_since
      else
        now
      end
    )
  end

  @doc """
  Decodes a JSON string (or map) into an `Event` struct.
  """
  @spec from_json!(String.t() | map()) :: t()
  def from_json!(json) when is_binary(json) do
    json
    |> Jason.decode!()
    |> from_json!()
  end

  def from_json!(%{} = map) do
    map
    |> JSON.decode_atom("slug")
    |> JSON.decode_atom("status")
    |> JSON.decode_polymorphic("error")
    |> JSON.decode_timestamp("initial_event_at")
    |> JSON.decode_timestamp("last_event_at")
    |> JSON.decode_timestamp("last_success_at")
    |> JSON.decode_timestamp("last_fail_at")
    |> JSON.decode_timestamp("in_status_since")
    |> JSON.struct(__MODULE__)
  end

  defimpl Jason.Encoder do
    def encode(%Event{} = event, opts) do
      # If you need special encoding logic (like your `Error` field),
      # you can do that here before delegating to `Jason.Encode.map/2`.
      event
      |> Map.from_struct()
      |> JSON.encode_polymorphic(:error)
      |> Jason.Encode.map(opts)
    end
  end
end
