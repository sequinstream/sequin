defmodule Sequin.Streams.StreamTable do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset

  alias Sequin.Accounts.Account
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Sources.PostgresReplication
  alias Sequin.Streams.StreamTableColumn

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

    has_many :columns, StreamTableColumn

    timestamps()
  end

  def changeset(%__MODULE__{} = stream_table, attrs) do
    stream_table
    |> cast(attrs, [
      :name,
      :table_schema_name,
      :table_name,
      :retention_policy,
      :insert_mode,
      :account_id,
      :source_postgres_database_id,
      :source_replication_slot_id
    ])
    |> validate_required([
      :name,
      :table_schema_name,
      :table_name,
      :retention_policy,
      :insert_mode,
      :account_id,
      :source_postgres_database_id,
      :source_replication_slot_id
    ])
    |> Sequin.Changeset.validate_name()
    |> validate_inclusion(:insert_mode, [:append, :upsert])
    |> foreign_key_constraint(:account_id)
    |> foreign_key_constraint(:source_postgres_database_id)
    |> foreign_key_constraint(:source_replication_slot_id)
    |> unique_constraint([:account_id, :name])
  end

  # defp base_query(query \\ __MODULE__) do
  #   from(st in query, as: :stream_table)
  # end
end
