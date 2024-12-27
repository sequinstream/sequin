defmodule Sequin.Sinks.Redis.ConnectionCache do
  @moduledoc """
  Cache connections to customer Redis instances.

  By caching these connections, we can avoid paying a significant startup
  penalty when performing multiple operations on the same Redis instance.

  Each `Sequin.Consumers.RedisSink` gets its own connection in the cache.

  The cache takes ownership of the Redis connections and is responsible for
  closing them when they are invalidated (or when the cache is stopped). Thus,
  callers should not call `GenServer.stop/1` on these connections.

  Cached connections are invalidated and recreated when their Redis sink's
  connection options change.

  The cache will detect dead connections and create new ones as needed.
  """

  use GenServer

  alias Sequin.Consumers.RedisSink
  alias Sequin.Error.NotFoundError

  require Logger

  defmodule Cache do
    @moduledoc false

    @type sink :: RedisSink.t()
    @type entry :: %{
            conn: pid() | atom(),
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
      entry = Map.get(cache, sink.connection_id)

      cond do
        is_nil(entry) ->
          {:error, :not_found}

        is_pid(entry.conn) and !Process.alive?(entry.conn) ->
          Logger.warning("Cached Redis connection was dead upon lookup", sink_id: sink.connection_id)
          {:error, :not_found}

        entry.options_hash != new_hash ->
          Logger.info("Cached Redis sink connection was stale", sink_id: sink.connection_id)
          {:error, :stale}

        true ->
          {:ok, entry.conn}
      end
    end

    @spec pop(t(), sink()) :: {pid() | nil, t()}
    def pop(cache, sink) do
      {entry, new_cache} = Map.pop(cache, sink.connection_id, nil)

      if entry, do: {entry.conn, new_cache}, else: {nil, new_cache}
    end

    @spec store(t(), sink(), pid()) :: t()
    def store(cache, sink, conn) do
      entry = %{conn: conn, options_hash: options_hash(sink)}
      Map.put(cache, sink.connection_id, entry)
    end

    defp options_hash(sink) do
      :erlang.phash2(RedisSink.redis_url(sink, obscure_password: false))
    end
  end

  defmodule State do
    @moduledoc false
    use TypedStruct

    alias Sequin.Consumers.RedisSink

    @type sink :: RedisSink.t()
    @type opt :: {:start_fn, State.start_function()} | {:stop_fn, State.stop_function()}
    @type start_function :: (sink() -> start_result())
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
      stop_fn = Keyword.get(opts, :stop_fn, &:eredis.stop/1)

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

    defp default_start(%RedisSink{} = sink) do
      :eredis.start_link(RedisSink.start_opts(sink))
    end
  end

  @type sink :: RedisSink.t()
  @type opt :: State.opt()
  @type start_result :: State.start_result()

  @spec start_link([opt]) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec connection(sink()) :: start_result()
  @spec connection(GenServer.server(), sink()) :: start_result()
  def connection(server \\ __MODULE__, %RedisSink{} = sink) do
    GenServer.call(server, {:connection, sink, true})
  end

  @spec existing_connection(GenServer.server(), sink()) :: start_result() | {:error, NotFoundError.t()}
  def existing_connection(server \\ __MODULE__, %RedisSink{} = sink) do
    GenServer.call(server, {:connection, sink, false})
  end

  @spec invalidate_connection(GenServer.server(), sink()) :: :ok
  def invalidate_connection(server \\ __MODULE__, %RedisSink{} = sink) do
    GenServer.cast(server, {:invalidate_connection, sink})
  end

  # This function is intended for test purposes only
  @spec cache_connection(GenServer.server(), sink(), pid()) :: :ok
  def cache_connection(server \\ __MODULE__, %RedisSink{} = sink, conn) do
    GenServer.call(server, {:cache_connection, sink, conn})
  end

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)

    {:ok, State.new(opts)}
  end

  @impl GenServer
  def handle_call({:connection, %RedisSink{} = sink, create_on_miss}, _from, %State{} = state) do
    case State.find_or_create_connection(state, sink, create_on_miss) do
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
  def handle_call({:cache_connection, %RedisSink{} = sink, conn}, _from, %State{} = state) do
    new_cache = Cache.store(state.cache, sink, conn)
    new_state = %{state | cache: new_cache}
    {:reply, :ok, new_state}
  end

  @impl GenServer
  def handle_cast({:invalidate_connection, %RedisSink{} = sink}, %State{} = state) do
    new_state = State.invalidate_connection(state, sink)
    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info({:EXIT, _pid, :normal}, %State{} = state) do
    # TODO: Do we need to handle connection going down? We're the ~only ones that can invalidate it?
    {:noreply, state}
  end

  @impl GenServer
  def terminate(_reason, %State{} = state) do
    _new_state = State.invalidate_all(state)
    :ok
  end
end
