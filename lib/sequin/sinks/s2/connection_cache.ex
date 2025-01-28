defmodule Sequin.Sinks.S2.ConnectionCache do
  @moduledoc false
  use GenServer

  alias Sequin.Consumers.S2Sink
  alias Sequin.Error.NotFoundError

  require Logger

  defmodule Cache do
    @moduledoc false

    @type sink :: S2Sink.t()
    @type entry :: %{
            channel: GRPC.Channel.t(),
            options_hash: binary()
          }
    @type t :: %{binary() => entry()}

    @spec new :: t()
    def new, do: %{}

    @spec each(t(), (GRPC.Channel.t() -> any())) :: :ok
    def each(cache, function) do
      Enum.each(cache, fn {_id, entry} -> function.(entry.channel) end)
    end

    @spec lookup(t(), sink()) :: {:ok, GRPC.Channel.t()} | {:error, :stale} | {:error, :not_found}
    def lookup(cache, sink) do
      new_hash = options_hash(sink)
      entry = Map.get(cache, sink.connection_id)

      cond do
        is_nil(entry) ->
          {:error, :not_found}

        is_pid(entry.channel) and !Process.alive?(entry.channel) ->
          Logger.warning("Cached S2 connection was dead upon lookup", sink_id: sink.connection_id)
          {:error, :not_found}

        entry.options_hash != new_hash ->
          Logger.info("Cached S2 sink connection was stale", sink_id: sink.connection_id)
          {:error, :stale}

        true ->
          {:ok, entry.channel}
      end
    end

    @spec pop(t(), sink()) :: {GRPC.Channel.t() | nil, t()}
    def pop(cache, sink) do
      {entry, new_cache} = Map.pop(cache, sink.connection_id, nil)

      if entry, do: {entry.channel, new_cache}, else: {nil, new_cache}
    end

    @spec store(t(), sink(), GRPC.Channel.t()) :: t()
    def store(cache, sink, channel) do
      entry = %{channel: channel, options_hash: options_hash(sink)}
      Map.put(cache, sink.connection_id, entry)
    end

    defp options_hash(sink) do
      :erlang.phash2({sink.token, sink.stream, sink.basin})
    end
  end

  defmodule State do
    @moduledoc false
    use TypedStruct

    alias Sequin.Consumers.S2Sink

    @type sink :: S2Sink.t()
    @type opt :: {:start_fn, State.start_function()} | {:stop_fn, State.stop_function()}
    @type start_function :: (sink() -> start_result())
    @type start_result ::
            {:ok, GRPC.Channel.t()}
            | {:error, term()}
    @type stop_function :: (GRPC.Channel.t() -> :ok)

    typedstruct do
      field :cache, Cache.t(), default: Cache.new()
      field :start_fn, start_function()
      field :stop_fn, stop_function()
    end

    @spec new([opt]) :: t()
    def new(opts) do
      start_fn = Keyword.get(opts, :start_fn, &default_start/1)
      stop_fn = Keyword.get(opts, :stop_fn, &GRPC.Stub.disconnect/1)

      %__MODULE__{
        start_fn: start_fn,
        stop_fn: stop_fn
      }
    end

    @spec find_or_create_connection(t(), sink(), boolean()) :: {:ok, GRPC.Channel.t(), t()} | {:error, term()}
    def find_or_create_connection(%__MODULE__{} = state, sink, create_on_miss) do
      case Cache.lookup(state.cache, sink) do
        {:ok, channel} ->
          {:ok, channel, state}

        {:error, :stale} ->
          state
          |> invalidate_connection(sink)
          |> find_or_create_connection(sink, create_on_miss)

        {:error, :not_found} when create_on_miss ->
          with {:ok, channel} <- state.start_fn.(sink) do
            new_cache = Cache.store(state.cache, sink, channel)
            new_state = %{state | cache: new_cache}
            {:ok, channel, new_state}
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

    defp default_start(%S2Sink{} = sink) do
      GRPC.Stub.connect(
        "#{sink.basin}.b.aws.s2.dev",
        "443",
        cred: GRPC.Credential.new(ssl: [cacerts: :public_key.cacerts_get()]),
        adapter: GRPC.Client.Adapters.Mint,
        interceptors: [GRPC.Client.Interceptors.Logger],
        headers: [{"Authorization", "Bearer #{sink.token}"}]
      )
    end
  end

  @type sink :: Sequin.Consumers.S2Sink.t()
  @type opt :: State.opt()
  @type start_result :: State.start_result()

  @spec start_link([opt]) :: GenServer.on_start()
  def start_link(opts) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @spec connection(sink()) :: start_result()
  @spec connection(GenServer.server(), sink()) :: start_result()
  def connection(server \\ __MODULE__, %S2Sink{} = sink) do
    GenServer.call(server, {:connection, sink, true})
  end

  @spec existing_connection(GenServer.server(), sink()) :: start_result() | {:error, NotFoundError.t()}
  def existing_connection(server \\ __MODULE__, %S2Sink{} = sink) do
    GenServer.call(server, {:connection, sink, false})
  end

  @spec invalidate_connection(GenServer.server(), sink()) :: :ok
  def invalidate_connection(server \\ __MODULE__, %S2Sink{} = sink) do
    GenServer.cast(server, {:invalidate_connection, sink})
  end

  # This function is intended for test purposes only
  @spec cache_connection(GenServer.server(), sink(), GRPC.Channel.t()) :: :ok
  def cache_connection(server \\ __MODULE__, %S2Sink{} = sink, channel) do
    GenServer.call(server, {:cache_connection, sink, channel})
  end

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)

    {:ok, State.new(opts)}
  end

  @impl GenServer
  def handle_call({:connection, %S2Sink{} = sink, create_on_miss}, _from, %State{} = state) do
    case State.find_or_create_connection(state, sink, create_on_miss) do
      {:ok, channel, new_state} ->
        {:reply, {:ok, channel}, new_state}

      {:error, :not_found} ->
        {:reply, {:error, Sequin.Error.not_found(entity: :database_connection)}, state}

      error ->
        {:reply, error, state}
    end
  end

  # This function is intended for test purposes only
  @impl GenServer
  def handle_call({:cache_connection, %S2Sink{} = sink, channel}, _from, %State{} = state) do
    new_cache = Cache.store(state.cache, sink, channel)
    new_state = %{state | cache: new_cache}
    {:reply, :ok, new_state}
  end

  @impl GenServer
  def handle_cast({:invalidate_connection, %S2Sink{} = sink}, %State{} = state) do
    new_state = State.invalidate_connection(state, sink)
    {:noreply, new_state}
  end

  @impl GenServer
  def terminate(_reason, %State{} = state) do
    _new_state = State.invalidate_all(state)
    :ok
  end
end
