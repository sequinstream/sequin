defmodule Sequin.Databases.PostgresDatabase do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias __MODULE__
  alias Ecto.Queryable
  alias Sequin.Databases.PostgresDatabase.Table
  alias Sequin.Replication.PostgresReplicationSlot

  require Logger

  @default_connect_timeout :timer.seconds(20)

  @derive {Jason.Encoder,
           only: [
             :id,
             :database,
             :hostname,
             :pool_size,
             :port,
             :queue_interval,
             :queue_target,
             :name,
             :ssl,
             :username,
             :password,
             :ipv6,
             :local_tunnel_id
           ]}
  @derive {Inspect, except: [:tables, :password]}
  typed_schema "postgres_databases" do
    field :database, :string
    field :hostname, :string
    field :pool_size, :integer, default: 3
    field :port, :integer
    field :queue_interval, :integer, default: 1000
    field :queue_target, :integer, default: 50
    field :name, :string
    field :ssl, :boolean, default: false
    field :username, :string
    field(:password, Sequin.Encrypted.Binary) :: String.t()
    field :tables_refreshed_at, :utc_datetime
    field :ipv6, :boolean, default: false

    embeds_many :tables, Table, on_replace: :delete, primary_key: false do
      field :oid, :integer, primary_key: true
      field :schema, :string
      field :name, :string
      field :sort_column_attnum, :integer, virtual: true

      embeds_many :columns, Column, on_replace: :delete, primary_key: false do
        field :attnum, :integer, primary_key: true
        field :is_pk?, :boolean
        field :name, :string
        field :type, :string
      end
    end

    field :health, :map, virtual: true

    belongs_to(:account, Sequin.Accounts.Account)
    belongs_to(:local_tunnel, Sequin.Accounts.LocalTunnel)
    has_one(:replication_slot, PostgresReplicationSlot, foreign_key: :postgres_database_id)

    timestamps()
  end

  def changeset(pd, attrs) do
    pd
    |> cast(attrs, [
      :database,
      :hostname,
      :name,
      :password,
      :pool_size,
      :port,
      :queue_interval,
      :queue_target,
      :ssl,
      :tables_refreshed_at,
      :username,
      :ipv6,
      :local_tunnel_id
    ])
    |> validate_required([:database, :hostname, :port, :username, :password, :name])
    |> validate_number(:port, greater_than_or_equal_to: 0, less_than_or_equal_to: 65_535)
    |> validate_not_supabase_pooled()
    |> Sequin.Changeset.validate_name()
    |> cast_embed(:tables, with: &tables_changeset/2, required: false)
    |> unique_constraint([:account_id, :name],
      name: :postgres_databases_account_id_name_index,
      message: "Database name must be unique",
      error_key: :name
    )
    |> Sequin.Changeset.validate_name()
  end

  def create_changeset(pd, attrs) do
    changeset(pd, attrs)
  end

  def update_changeset(pd, attrs) do
    changeset(pd, attrs)
  end

  defp validate_not_supabase_pooled(%Ecto.Changeset{valid?: false} = changeset), do: changeset

  defp validate_not_supabase_pooled(%Ecto.Changeset{valid?: true} = changeset) do
    hostname = get_field(changeset, :hostname)

    if not is_nil(hostname) and String.contains?(hostname, "pooler.supabase.com") do
      add_error(
        changeset,
        :hostname,
        "Supabase pooled connections are not supported. Please use a direct connection."
      )
    else
      changeset
    end
  end

  def tables_changeset(table, attrs) do
    table
    |> cast(attrs, [:oid, :schema, :name])
    |> validate_required([:oid, :schema, :name])
    |> cast_embed(:columns, with: &columns_changeset/2, required: true)
  end

  def columns_changeset(column, attrs) do
    column
    |> cast(attrs, [:attnum, :name, :type, :is_pk?])
    |> validate_required([:attnum, :name, :type, :is_pk?])
  end

  def tables_to_map(tables) do
    Enum.map(tables, fn table ->
      table
      |> Sequin.Map.from_ecto()
      |> Map.update!(:columns, fn columns ->
        Enum.map(columns, &Sequin.Map.from_ecto/1)
      end)
    end)
  end

  def cast_rows(%Table{} = table, rows) do
    Enum.map(rows, fn row ->
      Map.new(table.columns, fn col ->
        casted_val =
          if col.type == "uuid", do: UUID.binary_to_string!(row[col.name]), else: row[col.name]

        {col.name, casted_val}
      end)
    end)
  end

  @spec where_account(Queryable.t(), String.t()) :: Queryable.t()
  def where_account(query \\ base_query(), account_id) do
    from([database: pd] in query, where: pd.account_id == ^account_id)
  end

  @spec where_id(Queryable.t(), String.t()) :: Queryable.t()
  def where_id(query \\ base_query(), id) do
    from([database: pd] in query, where: pd.id == ^id)
  end

  def where_id_or_name(query \\ base_query(), id_or_name) do
    if Sequin.String.is_uuid?(id_or_name) do
      where_id(query, id_or_name)
    else
      where_name(query, id_or_name)
    end
  end

  def where_name(query \\ base_query(), name) do
    from([database: pd] in query, where: pd.name == ^name)
  end

  defp base_query(query \\ PostgresDatabase) do
    from(pd in query, as: :database)
  end

  def to_postgrex_opts(%PostgresDatabase{} = pd) do
    opts =
      pd
      |> Sequin.Map.from_ecto()
      |> Map.take([
        :database,
        :hostname,
        :pool_size,
        :port,
        :queue_interval,
        :queue_target,
        :password,
        :username,
        :connect_timeout,
        :max_restarts
      ])
      |> Enum.to_list()
      |> Keyword.put_new(:connect_timeout, @default_connect_timeout)

    # TODO: Remove this when we have CA certs for the cloud providers
    # We likely need a bundle that covers many different database providers
    # PLUS a path for users to provide their own certs if needed
    ssl =
      if pd.ssl do
        [
          verify: :verify_none
        ]
      else
        false
      end

    opts = Keyword.put(opts, :ssl, ssl)

    if pd.ipv6 do
      Keyword.put(opts, :socket_options, [:inet6])
    else
      opts
    end
  end
end
