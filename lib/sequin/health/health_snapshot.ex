defmodule Sequin.Health.HealthSnapshot do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset

  typed_schema "health_snapshots" do
    field :entity_id, :string
    field :entity_kind, Ecto.Enum, values: [:http_endpoint, :sink_consumer, :postgres_replication_slot, :wal_pipeline]
    field :status, Ecto.Enum, values: [:healthy, :warning, :error, :initializing, :waiting]
    field :health_json, :map
    field :sampled_at, :utc_datetime_usec

    timestamps()
  end

  def changeset(snapshot, attrs) do
    snapshot
    |> cast(attrs, [:entity_id, :entity_kind, :status, :health_json, :sampled_at])
    |> validate_required([:entity_id, :entity_kind, :status, :health_json, :sampled_at])
  end
end
