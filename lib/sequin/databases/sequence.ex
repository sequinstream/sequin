defmodule Sequin.Databases.Sequence do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Databases.PostgresDatabase

  @derive {Jason.Encoder,
           only: [:id, :name, :table_oid, :table_schema, :table_name, :sort_column_attnum, :sort_column_name]}
  typed_schema "sequences" do
    field :name, :string
    field :table_oid, :integer
    field :table_schema, :string
    field :table_name, :string
    field :sort_column_attnum, :integer
    field :sort_column_name, :string
    field :sort_column_type, :string, virtual: true

    belongs_to :account, Sequin.Accounts.Account
    belongs_to :postgres_database, PostgresDatabase
    has_many :sink_consumers, SinkConsumer

    timestamps()
  end

  def changeset(sequence, attrs) do
    sequence
    |> cast(attrs, [
      :name,
      :table_oid,
      :table_schema,
      :table_name,
      :sort_column_attnum,
      :sort_column_name,
      :postgres_database_id
    ])
    |> validate_required([
      :name,
      :table_oid,
      :postgres_database_id
    ])
    |> unique_constraint([:account_id, :name], error_key: :name)
    |> unique_constraint([:postgres_database_id, :table_oid], error_key: :table_oid)
  end

  def where_id(query \\ base_query(), id) do
    from(s in query, where: s.id == ^id)
  end

  def where_name(query \\ base_query(), name) do
    from(s in query, where: s.name == ^name)
  end

  def where_table_schema(query \\ base_query(), table_schema) do
    from(s in query, where: s.table_schema == ^table_schema)
  end

  def where_table_name(query \\ base_query(), table_name) do
    from(s in query, where: s.table_name == ^table_name)
  end

  def where_table_oid(query \\ base_query(), table_oid) do
    from(s in query, where: s.table_oid == ^table_oid)
  end

  def where_account(query \\ base_query(), account_id) do
    from(s in query,
      join: db in assoc(s, :postgres_database),
      where: db.account_id == ^account_id
    )
  end

  def where_postgres_database_id(query \\ base_query(), postgres_database_id) do
    from(s in query, where: s.postgres_database_id == ^postgres_database_id)
  end

  defp base_query do
    from(s in __MODULE__, as: :sequence)
  end
end
