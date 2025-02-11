defmodule Sequin.Replication.ReplicationSlotWatermarkWalCursor do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias Ecto.Queryable
  alias Sequin.Replication.PostgresReplicationSlot

  typed_schema "replication_slot_watermark_wal_cursor" do
    field :commit_lsn, :integer
    field :commit_idx, :integer
    field :boundary, Ecto.Enum, values: [:high, :low]

    belongs_to :replication_slot, PostgresReplicationSlot

    timestamps()
  end

  def changeset(watermark, attrs) do
    watermark
    |> cast(attrs, [:commit_lsn, :commit_idx])
    |> validate_required([:commit_lsn, :commit_idx])
    |> foreign_key_constraint(:replication_slot_id)
    |> unique_constraint([:replication_slot_id, :boundary])
  end

  @spec where_replication_slot(Queryable.t(), integer()) :: Queryable.t()
  def where_replication_slot(query \\ base_query(), replication_slot_id) do
    from([watermark: w] in query, where: w.replication_slot_id == ^replication_slot_id)
  end

  @spec where_boundary(Queryable.t(), atom()) :: Queryable.t()
  def where_boundary(query \\ base_query(), boundary) do
    from([watermark: w] in query, where: w.boundary == ^boundary)
  end

  defp base_query(query \\ __MODULE__) do
    from(w in query, as: :watermark)
  end
end
