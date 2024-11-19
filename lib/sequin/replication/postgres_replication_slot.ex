defmodule Sequin.Replication.PostgresReplicationSlot do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias Ecto.Queryable
  alias Sequin.Databases.PostgresDatabase

  defmodule Info do
    @moduledoc false
    use TypedStruct

    @derive Jason.Encoder
    typedstruct do
      field :last_committed_at, nil | DateTime.t()
      field :total_ingested_messages, nil | non_neg_integer()
    end
  end

  @derive {Jason.Encoder,
           only: [
             :id,
             :publication_name,
             :slot_name,
             :account_id,
             :postgres_database_id
           ]}
  typed_schema "postgres_replication_slots" do
    field :publication_name, :string
    field :slot_name, :string
    field :status, Ecto.Enum, values: [:active, :disabled], read_after_writes: true

    belongs_to :account, Sequin.Accounts.Account
    belongs_to :postgres_database, PostgresDatabase

    has_many :sink_consumers, Sequin.Consumers.SinkConsumer, foreign_key: :replication_slot_id
    has_many :wal_pipelines, Sequin.Replication.WalPipeline, foreign_key: :replication_slot_id

    field :info, :map, virtual: true

    timestamps()
  end

  def create_changeset(replication, attrs) do
    replication
    |> cast(attrs, [:publication_name, :slot_name, :postgres_database_id, :status])
    |> cast_assoc(:postgres_database,
      with: fn _struct, attrs ->
        PostgresDatabase.changeset(%PostgresDatabase{account_id: replication.account_id}, attrs)
      end
    )
    |> validate_required([:publication_name, :slot_name])
    |> unique_constraint([:slot_name, :postgres_database_id])
    |> foreign_key_constraint(:postgres_database_id, name: "postgres_replication_slots_postgres_database_id_fkey")
  end

  def update_changeset(replication, attrs) do
    replication
    |> cast(attrs, [:publication_name, :slot_name, :status])
    |> validate_required([:publication_name, :slot_name])
    |> unique_constraint([:slot_name, :postgres_database_id])
  end

  @spec where_account(Queryable.t(), String.t()) :: Queryable.t()
  def where_account(query \\ base_query(), account_id) do
    from([postgres_replication: pgr] in query, where: pgr.account_id == ^account_id)
  end

  @spec where_status(Queryable.t(), atom()) :: Queryable.t()
  def where_status(query \\ base_query(), status) do
    from([postgres_replication: pgr] in query, where: pgr.status == ^status)
  end

  defp base_query(query \\ __MODULE__) do
    from(pgr in query, as: :postgres_replication)
  end
end
