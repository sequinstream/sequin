defmodule Sequin.Streams.StreamTable do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias Sequin.Accounts.Account
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Replication.PostgresReplication
  alias Sequin.Streams.StreamTableColumn

  def base_columns do
    [
      {:sequin_id, :uuid, primary_key: true},
      {:seq, :bigserial, null: false},
      {:data, :text, []},
      {:recorded_at, :timestamp, null: false},
      {:deleted, :boolean, null: false}
    ]
  end

  def base_column_names do
    Enum.map(base_columns(), fn {name, _, _} -> name end)
  end

  @derive {Jason.Encoder,
           only: [
             :account_id,
             :id,
             :insert_mode,
             :inserted_at,
             :name,
             :retention_policy,
             :source_postgres_database_id,
             :source_replication_slot_id,
             :table_name,
             :table_schema_name,
             :updated_at
           ]}
  typed_schema "stream_tables" do
    field :insert_mode, Ecto.Enum, values: [:append, :upsert]
    field :name, :string
    field :retention_policy, :map
    field :table_name, :string
    field :table_schema_name, :string

    belongs_to :account, Account
    belongs_to :source_postgres_database, PostgresDatabase
    belongs_to :source_replication_slot, PostgresReplication

    has_many :columns, StreamTableColumn, on_replace: :delete

    timestamps()
  end

  def create_changeset(%__MODULE__{} = stream_table, attrs) do
    stream_table
    |> cast(attrs, [
      :name,
      :table_schema_name,
      :table_name,
      :retention_policy,
      :insert_mode,
      :source_postgres_database_id,
      :source_replication_slot_id
    ])
    |> validate_required([
      :name,
      :table_schema_name,
      :table_name,
      :retention_policy,
      :insert_mode,
      :source_postgres_database_id,
      :source_replication_slot_id
    ])
    |> validate_inclusion(:insert_mode, [:append, :upsert])
    |> foreign_key_constraint(:source_postgres_database_id)
    |> foreign_key_constraint(:source_replication_slot_id)
    |> unique_constraint([:account_id, :name], message: "has already been taken", error_key: :name)
    |> cast_assoc(:columns, with: &column_changeset/2)
    |> validate_primary_keys()
  end

  def update_changeset(%__MODULE__{} = stream_table, attrs) do
    stream_table
    |> cast(attrs, [
      :name,
      :table_schema_name,
      :table_name,
      :retention_policy
    ])
    |> validate_required([
      :name,
      :table_schema_name,
      :table_name,
      :retention_policy
    ])
    |> Sequin.Changeset.validate_name()
    |> cast_assoc(:columns, with: &column_changeset/2)
  end

  defp column_changeset(column, attrs) do
    if is_nil(column.id) do
      StreamTableColumn.create_changeset(column, attrs)
    else
      StreamTableColumn.update_changeset(column, attrs)
    end
  end

  defp validate_primary_keys(changeset) do
    insert_mode = get_field(changeset, :insert_mode)
    columns = get_field(changeset, :columns) || []

    case insert_mode do
      :append ->
        if Enum.any?(columns, & &1.is_conflict_key) do
          add_error(changeset, :columns, "cannot have conflict keys when insert_mode is :append")
        else
          changeset
        end

      :upsert ->
        if Enum.any?(columns, & &1.is_conflict_key) do
          changeset
        else
          add_error(changeset, :columns, "must have at least one conflict key when insert_mode is :upsert")
        end

      _ ->
        changeset
    end
  end

  def all_column_names(%__MODULE__{} = stream_table) do
    Enum.map(base_column_names(), &Atom.to_string/1) ++
      Enum.map(stream_table.columns, & &1.name)
  end

  def where_account_id(query \\ base_query(), account_id) do
    from(st in query, where: st.account_id == ^account_id)
  end

  def where_id(query \\ base_query(), id) do
    from(st in query, where: st.id == ^id)
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
