defmodule Sequin.Streams.StreamTable do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias Sequin.Accounts.Account
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Sources.PostgresReplication
  alias Sequin.Streams.StreamTable
  alias Sequin.Streams.StreamTableColumn

  @derive {Jason.Encoder,
           only: [
             :id,
             :account_id,
             :source_postgres_database_id,
             :source_replication_slot_id,
             :table_schema_name,
             :table_name,
             :name,
             :retention_policy,
             :insert_mode,
             :inserted_at,
             :updated_at
           ]}
  typed_schema "stream_tables" do
    field :table_schema_name, :string
    field :table_name, :string
    field :name, :string
    field :retention_policy, :map
    field :insert_mode, Ecto.Enum, values: [:append, :upsert]

    belongs_to :account, Account
    belongs_to :source_postgres_database, PostgresDatabase
    belongs_to :source_replication_slot, PostgresReplication
    has_many :stream_columns, StreamTableColumn

    timestamps()
  end

  def changeset(%StreamTable{} = stream_table, attrs) do
    stream_table
    |> cast(attrs, [
      :source_postgres_database_id,
      :source_replication_slot_id,
      :table_schema_name,
      :table_name,
      :name,
      :retention_policy,
      :insert_mode
    ])
    |> validate_required([
      :source_postgres_database_id,
      :source_replication_slot_id,
      :table_schema_name,
      :table_name,
      :name,
      :retention_policy,
      :insert_mode
    ])
    |> cast_assoc(:stream_columns)
    |> Sequin.Changeset.validate_name()
    |> foreign_key_constraint(:source_postgres_database_id)
    |> foreign_key_constraint(:source_replication_slot_id)
    |> unique_constraint([:account_id, :name], error_key: :name)
    |> unique_constraint([:source_postgres_database_id, :table_schema_name, :table_name])
  end

  def where_account_id(query \\ base_query(), account_id) do
    from(st in query, where: st.account_id == ^account_id)
  end

  def where_id(query \\ base_query(), id) do
    from(st in query, where: st.id == ^id)
  end

  def where_name(query \\ base_query(), name) do
    from(st in query, where: st.name == ^name)
  end

  def where_id_or_name(query \\ base_query(), id_or_name) do
    if Sequin.String.is_uuid?(id_or_name) do
      where_id(query, id_or_name)
    else
      where_name(query, id_or_name)
    end
  end

  def where_source_postgres_database_id(query \\ base_query(), source_postgres_database_id) do
    from(st in query, where: st.source_postgres_database_id == ^source_postgres_database_id)
  end

  def where_source_replication_slot_id(query \\ base_query(), source_replication_slot_id) do
    from(st in query, where: st.source_replication_slot_id == ^source_replication_slot_id)
  end

  defp base_query(query \\ __MODULE__) do
    from(st in query, as: :stream_table)
  end
end
