defmodule Sequin.Health.HealthSnapshot do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias Sequin.Health.HealthSnapshot

  typed_schema "health_snapshots" do
    field :entity_id, :string
    field :entity_name, :string
    field :entity_kind, Ecto.Enum, values: [:http_endpoint, :sink_consumer, :postgres_replication_slot, :wal_pipeline]
    field :status, Ecto.Enum, values: [:healthy, :warning, :error, :initializing, :waiting, :paused]
    field :health_json, :map
    field :sampled_at, :utc_datetime_usec

    timestamps()
  end

  def changeset(snapshot, attrs) do
    snapshot
    |> cast(attrs, [:entity_id, :entity_name, :entity_kind, :status, :health_json, :sampled_at])
    |> validate_required([:entity_id, :entity_kind, :status, :health_json, :sampled_at])
  end

  @doc """
  Query to get the latest health snapshot for each entity.
  Returns a query that can be further composed or executed.

  Only selects the fields needed for metrics reporting (not including health_json).
  Filters out snapshots older than 1 hours to avoid stale metrics.
  """
  def latest_snapshots_query do
    cutoff_time = DateTime.add(DateTime.utc_now(), -1, :hour)

    # Use a window function to get the latest snapshot for each entity
    # Only select the columns needed for metrics (omit health_json)
    from s in HealthSnapshot,
      where: s.sampled_at > ^cutoff_time,
      select: %{s | health_json: nil},
      distinct: [s.entity_id, s.entity_kind],
      order_by: [desc: s.sampled_at]
  end
end
