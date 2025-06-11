defmodule Sequin.Databases.PostgresDatabase do
  @moduledoc false
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query

  alias __MODULE__
  alias Ecto.Queryable
  alias Sequin.Databases.PostgresDatabasePrimary
  alias Sequin.Databases.PostgresDatabaseTable
  alias Sequin.Replication.PostgresReplicationSlot

  require Logger

  @type id :: String.t()

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
             :use_local_tunnel,
             :pg_major_version
           ]}
  @derive {Inspect, except: [:tables, :password]}
  typed_schema "postgres_databases" do
    field :database, :string
    field :hostname, :string
    field :pool_size, :integer, default: 10
    # can be auto-generated when use_local_tunnel is true
    field :port, :integer, read_after_writes: true
    field :queue_interval, :integer, default: 1000
    field :queue_target, :integer, default: 50
    field :name, :string
    field :ssl, :boolean, default: false
    field :username, :string
    field(:password, Sequin.Encrypted.Binary) :: String.t()
    field :tables_refreshed_at, :utc_datetime
    field :ipv6, :boolean, default: false
    field :use_local_tunnel, :boolean, default: false
    field :annotations, :map, default: %{}
    field :pg_major_version, :integer

    embeds_many :tables, PostgresDatabaseTable, on_replace: :delete

    field :health, :map, virtual: true

    embeds_one :primary, PostgresDatabasePrimary, on_replace: :update

    belongs_to(:account, Sequin.Accounts.Account)
    has_one(:replication_slot, PostgresReplicationSlot, foreign_key: :postgres_database_id)
    has_many(:wal_pipelines, through: [:replication_slot, :wal_pipelines])
    has_many(:sink_consumers, through: [:replication_slot, :sink_consumers])

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
      :use_local_tunnel,
      :annotations,
      :pg_major_version
    ])
    |> validate_required([:hostname, :database, :username, :password, :name])
    |> validate_number(:port, greater_than_or_equal_to: 0, less_than_or_equal_to: 65_535)
    |> validate_not_supabase_pooled()
    |> cast_embed(:tables, with: &PostgresDatabaseTable.changeset/2, required: false)
    |> cast_embed(:primary, with: &PostgresDatabasePrimary.changeset/2, required: false)
    |> unique_constraint([:account_id, :name],
      name: :postgres_databases_account_id_name_index,
      message: "Database name must be unique",
      error_key: :name
    )
    |> Sequin.Changeset.validate_name()
    |> foreign_key_constraint(:account_id, name: "postgres_databases_account_id_fkey")
    |> Sequin.Changeset.annotations_check_constraint()
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

    if not is_nil(hostname) and String.contains?(hostname, "pooler.supabase") do
      add_error(
        changeset,
        :hostname,
        "Supabase pooled connections are not supported. Please use a direct connection."
      )
    else
      changeset
    end
  end

  @spec where_account(Queryable.t(), String.t()) :: Queryable.t()
  def where_account(query \\ base_query(), account_id) do
    from([database: pd] in query, where: pd.account_id == ^account_id)
  end

  def where_use_local_tunnel(query \\ base_query()) do
    from([database: pd] in query, where: pd.use_local_tunnel == true)
  end

  @spec where_id(Queryable.t(), String.t()) :: Queryable.t()
  def where_id(query \\ base_query(), id) do
    from([database: pd] in query, where: pd.id == ^id)
  end

  def where_id_or_name(query \\ base_query(), id_or_name) do
    if Sequin.String.uuid?(id_or_name) do
      where_id(query, id_or_name)
    else
      where_name(query, id_or_name)
    end
  end

  def where_name(query \\ base_query(), name) do
    from([database: pd] in query, where: pd.name == ^name)
  end

  def join_replication_slot(query \\ base_query()) do
    from([database: pd] in query, join: replication_slot in assoc(pd, :replication_slot), as: :postgres_replication)
  end

  defp base_query(query \\ PostgresDatabase) do
    from(pd in query, as: :database)
  end

  def with_local_tunnel(%PostgresDatabase{use_local_tunnel: true} = pd) do
    %PostgresDatabase{pd | hostname: Application.get_env(:sequin, :portal_hostname)}
  end

  def with_local_tunnel(pd), do: pd

  def connection_url(%PostgresDatabase{} = pd) do
    "postgres://#{pd.username}:#{pd.password}@#{pd.hostname}:#{pd.port}/#{pd.database}"
  end

  def to_postgrex_opts(%PostgresDatabase{} = pd) do
    opts =
      pd
      |> with_local_tunnel()
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
      |> Keyword.put(:types, Sequin.Postgres.PostgrexTypes)
      # Temporary workaround to resolve Postgrex endpoint resolving issues if PGHOST is set to a socket or socket directory
      # keep this until a new Postgrex version (>0.20.0) is published including this commit
      # https://github.com/elixir-ecto/postgrex/commit/412b55567b6f0f3feb587e38466fcab047581c0f
      |> Keyword.put(:socket, nil)
      |> Keyword.put(:socket_dir, nil)
      |> Keyword.put(:endpoints, nil)

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

  def from_primary(pri) do
    %__MODULE__{
      database: pri.database,
      hostname: pri.hostname,
      password: pri.password,
      port: pri.port,
      ssl: pri.ssl,
      username: pri.username,
      ipv6: pri.ipv6,
      # Default to a smaller pool_size of 3 for primary connection pool
      # There should only ever be ~2 writers at a time: the SlotProcessorServer synchronously writes
      # WAL messages via the primary, as does the TableReaderServer
      pool_size: 3
    }
  end
end
