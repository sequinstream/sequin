defmodule Sequin.Sinks.Mysql.ConnectionCache do
  @moduledoc """
  Cache connections to customer MySQL instances.

  By caching these connections, we can avoid paying a significant startup
  penalty when performing multiple operations on the same MySQL instance.

  Each `Sequin.Consumers.MysqlSink` gets its own connection in the cache.

  The cache takes ownership of the MySQL connections and is responsible for
  closing them when they are invalidated (or when the cache is stopped). Thus,
  callers should not call `GenServer.stop/1` on these connections.

  Cached connections are invalidated and recreated when their MySQL sink's
  connection options change.

  The cache will detect dead connections and create new ones as needed.
  """

  use GenServer

  alias Sequin.Consumers.MysqlSink

  require Logger

  defmodule Cache do
    @moduledoc false

    @type sink :: MysqlSink.t()
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

    @spec lookup(t(), sink()) :: {:ok, pid()} | {:error, :stale} | {:error, :not_found}
    def lookup(cache, sink) do
      new_hash = options_hash(sink)
      entry = Map.get(cache, MysqlSink.connection_id(sink))

      cond do
        is_nil(entry) ->
          {:error, :not_found}

        is_pid(entry.conn) and !Process.alive?(entry.conn) ->
          Logger.warning("Cached MySQL connection was dead upon lookup", sink_id: MysqlSink.connection_id(sink))
          {:error, :not_found}

        entry.options_hash != new_hash ->
          Logger.info("Cached MySQL sink connection was stale", sink_id: MysqlSink.connection_id(sink))
          {:error, :stale}

        true ->
          {:ok, entry.conn}
      end
    end

    @spec pop(t(), sink()) :: {pid() | nil, t()}
    def pop(cache, sink) do
      {entry, new_cache} = Map.pop(cache, MysqlSink.connection_id(sink), nil)

      if entry, do: {entry.conn, new_cache}, else: {nil, new_cache}
    end

    @spec store(t(), sink(), pid()) :: t()
    def store(cache, sink, conn) do
      entry = %{conn: conn, options_hash: options_hash(sink)}
      Map.put(cache, MysqlSink.connection_id(sink), entry)
    end

    defp options_hash(%MysqlSink{} = sink) do
      :erlang.phash2(MysqlSink.connection_opts(sink))
    end
  end

  defmodule State do
    @moduledoc false
    use TypedStruct

    alias Sequin.Error

    @type sink :: MysqlSink.t()
    @type opt :: {:start_fn, State.start_function()} | {:stop_fn, State.stop_function()}
    @type start_function :: (sink() -> start_result())
    @type start_result :: {:ok, pid()} | {:error, Error.t()}
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

    @spec find_or_create_connection(t(), sink(), boolean()) :: {:ok, pid(), t()} | {:error, term()}
    def find_or_create_connection(%__MODULE__{} = state, sink, create_on_miss) do
      case Cache.lookup(state.cache, sink) do
        {:ok, conn} ->
          {:ok, conn, state}

        {:error, :stale} ->
          state
          |> invalidate_connection(sink)
          |> find_or_create_connection(sink, create_on_miss)

        {:error, :not_found} when create_on_miss ->
          with {:ok, conn} <- state.start_fn.(sink) do
            new_cache = Cache.store(state.cache, sink, conn)
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

    @spec invalidate_connection(t(), sink()) :: t()
    def invalidate_connection(%__MODULE__{} = state, sink) do
      {conn, new_cache} = Cache.pop(state.cache, sink)

      if conn, do: state.stop_fn.(conn)

      %{state | cache: new_cache}
    end

    defp default_start(%MysqlSink{} = sink) do
      MyXQL.start_link(MysqlSink.connection_opts(sink))
    end
  end

  ## GenServer API

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @spec get_connection(MysqlSink.t()) :: {:ok, pid()} | {:error, term()}
  def get_connection(%MysqlSink{} = sink) do
    GenServer.call(__MODULE__, {:get_connection, sink})
  end

  @spec test_connection(MysqlSink.t()) :: :ok | {:error, term()}
  def test_connection(%MysqlSink{} = sink) do
    GenServer.call(__MODULE__, {:test_connection, sink})
  end

  @spec invalidate(MysqlSink.t()) :: :ok
  def invalidate(%MysqlSink{} = sink) do
    GenServer.cast(__MODULE__, {:invalidate, sink})
  end

  @spec invalidate_all :: :ok
  def invalidate_all do
    GenServer.cast(__MODULE__, :invalidate_all)
  end

  ## GenServer callbacks

  @impl GenServer
  def init(opts) do
    {:ok, State.new(opts)}
  end

  @impl GenServer
  def handle_call({:get_connection, sink}, _from, state) do
    case State.find_or_create_connection(state, sink, true) do
      {:ok, conn, new_state} ->
        {:reply, {:ok, conn}, new_state}

      {:error, error} ->
        Logger.error("Failed to get MySQL connection: #{inspect(error)}")
        {:reply, {:error, error}, state}
    end
  end

  @impl GenServer
  def handle_call({:test_connection, sink}, _from, state) do
    case State.find_or_create_connection(state, sink, true) do
      {:ok, conn, new_state} ->
        case MyXQL.query(conn, "SELECT 1", []) do
          {:ok, _result} ->
            {:reply, :ok, new_state}

          {:error, error} ->
            # Invalidate the connection on test failure
            invalidated_state = State.invalidate_connection(new_state, sink)
            {:reply, {:error, error}, invalidated_state}
        end

      {:error, error} ->
        {:reply, {:error, error}, state}
    end
  end

  @impl GenServer
  def handle_cast({:invalidate, sink}, state) do
    new_state = State.invalidate_connection(state, sink)
    {:noreply, new_state}
  end

  @impl GenServer
  def handle_cast(:invalidate_all, state) do
    new_state = State.invalidate_all(state)
    {:noreply, new_state}
  end
end
