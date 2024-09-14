defmodule Sequin.Databases.ConnectionCache do
  @moduledoc """
  Cache connections to customer databases.

  By caching these connections, we can avoid paying a significant startup
  penalty when performing multiple operations on the same database.

  Each `Sequin.Databases.PostgresDatabase` gets its own connection in the cache.

  The cache takes ownership of the database connections and is responsible for
  closing them when they are invalidated (or when the cache is stopped). Thus,
  callers should not call `GenServer.stop/1` on these connections.

  Cached connections are invalidated and recreated when their database's
  connection options change.

  The cache will detect dead connections and create new ones as needed.
  """

  use GenServer

  alias Sequin.Databases.PostgresDatabase
  alias Sequin.Error.NotFoundError

  require Logger

  defmodule Cache do
    @moduledoc false

    @type database :: PostgresDatabase.t()
    @type entry :: %{
            conn: pid(),
            options_hash: binary()
          }
    @type t :: %{binary() => entry()}

    @spec new :: t()
    def new, do: %{}

    @spec each(t(), (pid() -> any())) :: :ok
    def each(cache, function) do
      Enum.each(cache, fn {_id, entry} -> function.(entry.conn) end)
    end

    @spec lookup(t(), database()) :: {:ok, pid()} | {:error, :stale} | {:error, :not_found}
    def lookup(cache, db) do
      new_hash = options_hash(db)
      entry = Map.get(cache, db.id)

      cond do
        is_nil(entry) ->
          {:error, :not_found}

        !Process.alive?(entry.conn) ->
          Logger.warning("Cached db connection was dead upon lookup", database_id: db.id)
          {:error, :not_found}

        entry.options_hash != new_hash ->
          Logger.info("Cached db connection was stale", database_id: db.id)
          {:error, :stale}

        true ->
          {:ok, entry.conn}
      end
    end

    @spec pop(t(), database()) :: {pid() | nil, t()}
    def pop(cache, db) do
      {entry, new_cache} = Map.pop(cache, db.id, nil)

      if entry, do: {entry.conn, new_cache}, else: {nil, new_cache}
    end

    @spec store(t(), database(), pid()) :: t()
    def store(cache, db, conn) do
      entry = %{conn: conn, options_hash: options_hash(db)}
      Map.put(cache, db.id, entry)
    end

    defp options_hash(db) do
      config_hash = :erlang.phash2(PostgresDatabase.to_postgrex_opts(db))

      Enum.join([db.hostname, config_hash], ":")
    end
  end

  defmodule State do
    @moduledoc false
    use TypedStruct

    alias Sequin.Databases

    @type database :: PostgresDatabase.t()
    @type opt :: {:start_fn, State.start_function()} | {:stop_fn, State.stop_function()}
    @type start_function :: (database() -> start_result())
    @type start_result ::
            {:ok, pid()}
            | {:error, Postgrex.Error.t()}
    @type stop_function :: (pid() -> :ok)

    typedstruct do
      field :cache, Cache.t(), default: Cache.new()
      field :start_fn, start_function()
      field :stop_fn, stop_function()
    end

    @spec new([opt]) :: t()
    def new(opts) do
      start_fn = Keyword.get(opts, :start_fn, &default_start/1)
      stop_fn = Keyword.get(opts, :stop_fn, &GenServer.stop/1)

      %__MODULE__{
        start_fn: start_fn,
        stop_fn: stop_fn
      }
    end

    @spec find_or_create_connection(t(), database(), boolean()) :: {:ok, pid(), t()} | {:error, term()}
    def find_or_create_connection(%__MODULE__{} = state, db, create_on_miss) do
      case Cache.lookup(state.cache, db) do
        {:ok, conn} ->
          {:ok, conn, state}

        {:error, :stale} ->
          state
          |> invalidate_connection(db)
          |> find_or_create_connection(db, create_on_miss)

        {:error, :not_found} when create_on_miss ->
          with {:ok, conn} <- state.start_fn.(db) do
            new_cache = Cache.store(state.cache, db, conn)
            new_state = %{state | cache: new_cache}
            {:ok, conn, new_state}
          end

        {:error, :not_found} ->
          {:error, :not_found}
      end
    end

    @spec invalidate_all(t()) :: t()
    def invalidate_all(%__MODULE__{} = state) do
      Cache.each(state.cache, state.stop_fn)

      %{state | cache: Cache.new()}
    end

    @spec invalidate_connection(t(), database()) :: t()
    def invalidate_connection(%__MODULE__{} = state, db) do
      {conn, new_cache} = Cache.pop(state.cache, db)

      if conn, do: state.stop_fn.(conn)

      %{state | cache: new_cache}
    end

    defp default_start(%PostgresDatabase{} = db) do
      Databases.start_link(db)
    end
  end

  @type database :: PostgresDatabase.t()
  @type opt :: State.opt()
  @type start_result :: State.start_result()

  @spec start_link([opt]) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec connection(database()) :: start_result()
  @spec connection(GenServer.server(), database()) :: start_result()
  def connection(server \\ __MODULE__, %PostgresDatabase{} = db) do
    GenServer.call(server, {:connection, db, true})
  end

  @spec existing_connection(GenServer.server(), database()) :: start_result() | {:error, NotFoundError.t()}
  def existing_connection(server \\ __MODULE__, %PostgresDatabase{} = db) do
    GenServer.call(server, {:connection, db, false})
  end

  @spec invalidate_connection(GenServer.server(), database()) :: :ok
  def invalidate_connection(server \\ __MODULE__, %PostgresDatabase{} = db) do
    GenServer.cast(server, {:invalidate_connection, db})
  end

  # This function is intended for test purposes only
  @spec cache_connection(GenServer.server(), database(), pid()) :: :ok
  def cache_connection(server \\ __MODULE__, %PostgresDatabase{} = db, conn) do
    GenServer.call(server, {:cache_connection, db, conn})
  end

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)

    {:ok, State.new(opts)}
  end

  @impl GenServer
  def handle_call({:connection, %PostgresDatabase{} = db, create_on_miss}, _from, %State{} = state) do
    case State.find_or_create_connection(state, db, create_on_miss) do
      {:ok, conn, new_state} ->
        {:reply, {:ok, conn}, new_state}

      {:error, :not_found} ->
        {:reply, {:error, Sequin.Error.not_found(entity: :database_connection)}, state}

      error ->
        {:reply, error, state}
    end
  end

  # This function is intended for test purposes only
  @impl GenServer
  def handle_call({:cache_connection, %PostgresDatabase{} = db, conn}, _from, %State{} = state) do
    new_cache = Cache.store(state.cache, db, conn)
    new_state = %{state | cache: new_cache}
    {:reply, :ok, new_state}
  end

  @impl GenServer
  def handle_cast({:invalidate_connection, %PostgresDatabase{} = db}, %State{} = state) do
    new_state = State.invalidate_connection(state, db)
    {:noreply, new_state}
  end

  @impl GenServer
  def terminate(_reason, %State{} = state) do
    _new_state = State.invalidate_all(state)
    :ok
  end
end
