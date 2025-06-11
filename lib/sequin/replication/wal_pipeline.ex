defmodule Sequin.Replication.WalPipeline do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias Sequin.WalPipeline.SourceTable

  @derive {Jason.Encoder, only: [:id, :name, :status, :seq, :destination_oid, :destination_database_id, :source_tables]}
  typed_schema "wal_pipelines" do
    field :status, Ecto.Enum, values: [:active, :disabled], default: :active
    field :name, :string
    field :seq, :integer
    field :destination_oid, :integer

    field :health, :map, virtual: true
    field :annotations, :map, default: %{}

    embeds_many :source_tables, SourceTable, on_replace: :delete
    belongs_to :replication_slot, Sequin.Replication.PostgresReplicationSlot
    has_one :source_database, through: [:replication_slot, :postgres_database]
    belongs_to :destination_database, Sequin.Databases.PostgresDatabase
    belongs_to :account, Sequin.Accounts.Account

    timestamps()
  end

  @doc false
  def create_changeset(wal_pipeline, attrs) do
    wal_pipeline
    |> cast(attrs, [:name, :status, :seq, :replication_slot_id, :destination_oid, :destination_database_id, :annotations])
    |> changeset()
  end

  @doc false
  def update_changeset(wal_pipeline, attrs) do
    wal_pipeline
    |> cast(attrs, [:name, :status, :seq, :annotations])
    |> changeset()
  end

  defp changeset(wal_pipeline) do
    wal_pipeline
    |> Sequin.Changeset.cast_embed(:source_tables)
    |> validate_required([:name, :replication_slot_id, :destination_oid, :destination_database_id])
    |> unique_constraint([:replication_slot_id, :name])
    |> foreign_key_constraint(:replication_slot_id)
    |> foreign_key_constraint(:destination_database_id)
    |> Sequin.Changeset.annotations_check_constraint()
  end

  def where_account_id(query \\ base_query(), account_id) do
    from([wal_pipeline: wp] in query, where: wp.account_id == ^account_id)
  end

  def where_id_or_name(query \\ base_query(), id_or_name) do
    if Sequin.String.uuid?(id_or_name) do
      where_id(query, id_or_name)
    else
      where_name(query, id_or_name)
    end
  end

  def where_id(query \\ base_query(), id) do
    from([wal_pipeline: wp] in query, where: wp.id == ^id)
  end

  def where_name(query \\ base_query(), name) do
    from([wal_pipeline: wp] in query, where: wp.name == ^name)
  end

  def where_active(query \\ base_query()) do
    from([wal_pipeline: wp] in query, where: wp.status == :active)
  end

  def base_query(query \\ __MODULE__) do
    from(wp in query, as: :wal_pipeline)
  end

  def where_replication_slot_id(query \\ base_query(), replication_slot_id) do
    from([wal_pipeline: wp] in query, where: wp.replication_slot_id == ^replication_slot_id)
  end
end
