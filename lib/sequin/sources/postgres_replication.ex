defmodule Sequin.Sources.PostgresReplication do
  @moduledoc false
  use Sequin.Schema

  import Ecto.Changeset
  import Ecto.Query

  alias Ecto.Queryable

  schema "postgres_replications" do
    field :slot_name, :string
    field :publication_name, :string
    field :status, Ecto.Enum, values: [:active, :disabled], default: :active

    belongs_to :account, Sequin.Accounts.Account
    belongs_to :postgres_database, Sequin.Databases.PostgresDatabase
    belongs_to :stream, Sequin.Streams.Stream

    timestamps()
  end

  def changeset(replication, attrs) do
    replication
    |> cast(attrs, [:slot_name, :publication_name, :status, :postgres_database_id, :stream_id])
    |> validate_required([:slot_name, :publication_name, :postgres_database_id, :stream_id])
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
