defmodule Sequin.Sources.PostgresReplication do
  @moduledoc false
  use Sequin.Schema

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
    end
  end

  @derive {Jason.Encoder,
           only: [:id, :slot_name, :publication_name, :status, :account_id, :postgres_database_id, :stream_id]}
  schema "postgres_replications" do
    field :slot_name, :string
    field :publication_name, :string
    field :status, Ecto.Enum, values: [:active, :disabled], default: :active

    belongs_to :account, Sequin.Accounts.Account
    belongs_to :postgres_database, PostgresDatabase
    belongs_to :stream, Sequin.Streams.Stream

    field :info, :map, virtual: true

    timestamps()
  end

  def create_changeset(replication, attrs) do
    replication
    |> cast(attrs, [:slot_name, :publication_name, :status, :stream_id, :postgres_database_id])
    |> cast_assoc(:postgres_database,
      with: fn _struct, attrs ->
        PostgresDatabase.changeset(%PostgresDatabase{account_id: replication.account_id}, attrs)
      end
    )
    |> validate_required([:slot_name, :publication_name, :stream_id])
    |> unique_constraint([:slot_name, :postgres_database_id])
    |> foreign_key_constraint(:postgres_database_id, name: "postgres_replications_postgres_database_id_fkey")
    |> foreign_key_constraint(:stream_id, name: "postgres_replications_stream_id_fkey")
  end

  def update_changeset(replication, attrs) do
    replication
    |> cast(attrs, [:slot_name, :publication_name, :status])
    |> validate_required([:slot_name, :publication_name])
    |> unique_constraint([:slot_name, :postgres_database_id])
  end

  @spec where_active(Queryable.t()) :: Queryable.t()
  def where_active(query \\ base_query()) do
    from([postgres_replication: pgr] in query, where: pgr.status == :active)
  end

  @spec where_account(Queryable.t(), String.t()) :: Queryable.t()
  def where_account(query \\ base_query(), account_id) do
    from([postgres_replication: pgr] in query, where: pgr.account_id == ^account_id)
  end

  @spec where_stream(Queryable.t(), String.t()) :: Queryable.t()
  def where_stream(query \\ base_query(), stream_id) do
    from([postgres_replication: pgr] in query, where: pgr.stream_id == ^stream_id)
  end

  defp base_query(query \\ __MODULE__) do
    from(pgr in query, as: :postgres_replication)
  end
end
