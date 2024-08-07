defmodule Sequin.Streams.SourceTable do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset

  alias Sequin.Accounts.Account
  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Streams.SourceTableStreamTableColumnMapping

  @derive {Jason.Encoder,
           only: [
             :account_id,
             :id,
             :postgres_database_id,
             :schema_oid,
             :oid,
             :schema_name,
             :name,
             :inserted_at,
             :updated_at
           ]}
  typed_schema "source_tables" do
    field :schema_oid, :integer
    field :oid, :integer
    field :schema_name, :string
    field :name, :string

    belongs_to :account, Account
    belongs_to :postgres_database, PostgresDatabase

    has_many :column_mappings, SourceTableStreamTableColumnMapping

    timestamps()
  end

  def changeset(%__MODULE__{} = source_table, attrs) do
    source_table
    |> cast(attrs, [
      :account_id,
      :postgres_database_id,
      :schema_oid,
      :oid,
      :schema_name,
      :name
    ])
    |> validate_required([
      :account_id,
      :postgres_database_id,
      :schema_oid,
      :oid
    ])
    |> foreign_key_constraint(:account_id)
    |> foreign_key_constraint(:postgres_database_id)
    |> unique_constraint([:postgres_database_id, :oid])
  end

  # defp base_query(query \\ __MODULE__) do
  #   from(st in query, as: :source_table)
  # end
end
