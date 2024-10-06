defmodule Sequin.Replication.WalProjection do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias Sequin.Consumers.SourceTable

  schema "wal_projections" do
    field :status, Ecto.Enum, values: [:active, :disabled], default: :active
    field :name, :string
    field :seq, :integer
    field :destination_oid, :integer

    embeds_many :source_tables, SourceTable, on_replace: :delete
    belongs_to :replication_slot, Sequin.Replication.PostgresReplicationSlot
    has_one :source_database, through: [:replication_slot, :postgres_database]
    belongs_to :destination_database, Sequin.Databases.PostgresDatabase
    belongs_to :account, Sequin.Accounts.Account

    timestamps()
  end

  @doc false
  def create_changeset(wal_projection, attrs) do
    changeset(wal_projection, attrs)
  end

  @doc false
  def update_changeset(wal_projection, attrs) do
    changeset(wal_projection, attrs)
  end

  defp changeset(wal_projection, attrs) do
    wal_projection
    |> cast(attrs, [:name, :status, :seq, :replication_slot_id, :destination_oid, :destination_database_id])
    |> Sequin.Changeset.cast_embed(:source_tables)
    |> validate_required([:name, :replication_slot_id, :destination_oid, :destination_database_id])
    |> unique_constraint([:replication_slot_id, :name])
    |> foreign_key_constraint(:replication_slot_id)
    |> foreign_key_constraint(:destination_database_id)
  end

  def base_query(query \\ __MODULE__) do
    from(wp in query, as: :wal_projection)
  end

  def where_replication_slot_id(query \\ base_query(), replication_slot_id) do
    from([wal_projection: wp] in query, where: wp.replication_slot_id == ^replication_slot_id)
  end
end
