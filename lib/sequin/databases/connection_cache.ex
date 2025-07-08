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
  alias Sequin.Error
  alias Sequin.Error.NotFoundError

  require Logger

  defmodule Cache do
    @moduledoc false

    @type conn :: pid() | atom()
    @type database :: PostgresDatabase.t()
    @type entry :: %{
            conn: conn(),
            hash: binary()
          }
    @type t :: %{binary() => entry()}
    @type database_hash :: String.t()

    @spec new :: t()
    def new, do: %{}

    @spec each(t(), (pid() -> any())) :: :ok
    def each(cache, function) do
      Enum.each(cache, fn {_id, entry} -> function.(entry.conn) end)
    end

    @spec lookup(t(), PostgresDatabase.id(), database_hash()) :: {:ok, pid()} | {:error, :stale} | {:error, :not_found}
    def lookup(cache, db_id, new_hash) do
      entry = Map.get(cache, db_id)

      cond do
        is_nil(entry) ->
          {:error, :not_found}

        is_pid(entry.conn) and !Process.alive?(entry.conn) ->
          Logger.warning("[ConnectionCache] Cached db connection was dead upon lookup", database_id: db_id)
          {:error, :not_found}

        entry.hash != new_hash ->
          Logger.info("[ConnectionCache] Cached db connection was stale", database_id: db_id)
          {:error, :stale}

        true ->
          {:ok, entry.conn}
      end
    end

    @spec pop(t(), PostgresDatabase.id()) :: {conn() | nil, t()}
    def pop(cache, db_id) do
      {entry, new_cache} = Map.pop(cache, db_id, nil)

      if entry, do: {entry.conn, new_cache}, else: {nil, new_cache}
    end

    @spec store(t(), PostgresDatabase.id(), database_hash(), pid()) :: t()
    def store(cache, db_id, hash, conn) do
      entry = %{conn: conn, hash: hash}
      Map.put(cache, db_id, entry)
    end
  end

  defmodule State do
    @moduledoc false
    use TypedStruct

    alias Sequin.Databases

    @type database :: PostgresDatabase.t()
    @type database_hash :: String.t()
    @type opt :: {:start_fn, State.start_function()} | {:stop_fn, State.stop_function()}
    @type start_function :: (database() -> start_result())
    @type start_result ::
            {:ok, pid()}
            | {:error, Postgrex.Error.t()}
    @type stop_function :: (pid() -> :ok)
    @type start_task :: %{
            db: database(),
            hash: database_hash(),
            ref: reference(),
            listeners: [GenServer.from()]
          }

    typedstruct do
      field :cache, Cache.t(), default: Cache.new()
      field :start_fn, start_function()
      field :stop_fn, stop_function()
      field :start_tasks, [start_task()], default: []
    end

    @spec new([opt]) :: t()
    def new(opts) do
      start_fn = Keyword.get(opts, :start_fn, &default_start/1)
      stop_fn = Keyword.get(opts, :stop_fn, &default_stop/1)

      %__MODULE__{
        start_fn: start_fn,
        stop_fn: stop_fn
      }
    end

    @spec find_connection(t(), database()) :: {:ok, pid(), t()} | {:error, term()}
    def find_connection(%__MODULE__{} = state, db) do
      case Cache.lookup(state.cache, db.id, options_hash(db)) do
        {:ok, conn} ->
          {:ok, conn, state}

        {:error, :stale} ->
          invalidate_connection(state, db)
          {:error, :not_found}

        {:error, :not_found} ->
          {:error, :not_found}
      end
    end

    @spec store_connection(t(), database(), pid()) :: t()
    def store_connection(%__MODULE__{} = state, db, conn) do
      cache = Cache.store(state.cache, db.id, options_hash(db), conn)
      %{state | cache: cache}
    end

    @spec invalidate_all(t()) :: t()
    def invalidate_all(%__MODULE__{} = state) do
      Cache.each(state.cache, state.stop_fn)

      %{state | cache: Cache.new()}
    end

    @spec invalidate_connection(t(), database()) :: t()
    def invalidate_connection(%__MODULE__{} = state, db) do
      {conn, new_cache} = Cache.pop(state.cache, db.id)

      # We don't want to accidentally kill the Sequin.Repo connection, which we can store in the
      # ConnectionCache during test. Leads to very hard to debug error!
      if not is_nil(conn) and conn not in [Sequin.Repo, Sequin.Test.UnboxedRepo] do
        state.stop_fn.(conn)
      end

      %{state | cache: new_cache}
    end

    @spec start_task_ref(t(), database()) :: reference() | nil
    def start_task_ref(%__MODULE__{} = state, db) do
      start_task =
        Enum.find(state.start_tasks, fn start_task ->
          start_task.db.id == db.id and start_task.hash == options_hash(db)
        end)

      if start_task, do: start_task.ref
    end

    @spec put_start_task_listener(t(), reference(), GenServer.from()) :: t()
    def put_start_task_listener(%__MODULE__{} = state, ref, from) do
      start_task_index = Enum.find_index(state.start_tasks, fn %{ref: r} -> r == ref end)

      if is_nil(start_task_index) do
        raise ArgumentError, "Start task not found"
      end

      start_task = Enum.at(state.start_tasks, start_task_index)
      start_task = %{start_task | listeners: [from | start_task.listeners]}
      start_tasks = List.replace_at(state.start_tasks, start_task_index, start_task)
      %{state | start_tasks: start_tasks}
    end

    @spec put_start_task(t(), database(), reference()) :: t()
    def put_start_task(%__MODULE__{} = state, db, ref) do
      %{state | start_tasks: [%{db: db, hash: options_hash(db), ref: ref, listeners: []} | state.start_tasks]}
    end

    @spec pop_start_task(t(), reference()) :: {start_task() | nil, t()}
    def pop_start_task(%__MODULE__{} = state, ref) do
      case Enum.split_with(state.start_tasks, fn %{ref: r} -> r == ref end) do
        {[start_task], rest_start_tasks} ->
          {start_task, %{state | start_tasks: rest_start_tasks}}

        {[], _rest} ->
          {nil, state}
      end
    end

    defp options_hash(%PostgresDatabase{} = db) do
      config_hash = :erlang.phash2(PostgresDatabase.to_postgrex_opts(db))

      Enum.join([db.hostname, config_hash], ":")
    end

    defp default_start(%PostgresDatabase{} = db) do
      Databases.start_link(db)
    end

    defp default_stop(conn) do
      Task.Supervisor.async_nolink(Sequin.TaskSupervisor, fn ->
        if Process.alive?(conn) do
          GenServer.stop(conn)
        end
      end)

      :ok
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
  def connection(server \\ __MODULE__, %PostgresDatabase{id: id} = db) when is_binary(id) do
    GenServer.call(server, {:connection, db, true})
  end

  @spec existing_connection(GenServer.server(), database()) :: start_result() | {:error, NotFoundError.t()}
  def existing_connection(server \\ __MODULE__, %PostgresDatabase{id: id} = db) when is_binary(id) do
    GenServer.call(server, {:connection, db, false})
  end

  @spec invalidate_connection(GenServer.server(), database()) :: :ok
  def invalidate_connection(server \\ __MODULE__, %PostgresDatabase{id: id} = db) when is_binary(id) do
    GenServer.cast(server, {:invalidate_connection, db})
  end

  # This function is intended for test purposes only
  @spec cache_connection(GenServer.server(), database(), pid()) :: :ok
  def cache_connection(server \\ __MODULE__, %PostgresDatabase{id: id} = db, conn) when is_binary(id) do
    GenServer.call(server, {:cache_connection, db, conn})
  end

  @impl GenServer
  def init(opts) do
    Process.flag(:trap_exit, true)

    {:ok, State.new(opts)}
  end

  @impl GenServer
  def handle_call({:connection, %PostgresDatabase{} = db, create_on_miss}, from, %State{} = state) do
    case State.find_connection(state, db) do
      {:ok, conn, new_state} ->
        {:reply, {:ok, conn}, new_state}

      {:error, :not_found} when create_on_miss ->
        if ref = State.start_task_ref(state, db) do
          state = State.put_start_task_listener(state, ref, from)
          {:noreply, state}
        else
          %{ref: task_ref} =
            Task.Supervisor.async_nolink(
              Sequin.TaskSupervisor,
              fn ->
                {:start_result, state.start_fn.(db)}
              end,
              timeout: to_timeout(second: 30)
            )

          state =
            state
            |> State.put_start_task(db, task_ref)
            |> State.put_start_task_listener(task_ref, from)

          {:noreply, state}
        end

      {:error, :not_found} ->
        error = Error.not_found(entity: :customer_postgres, params: %{database_id: db.id})
        {:reply, {:error, error}, state}
    end
  end

  # This function is intended for test purposes only
  @impl GenServer
  def handle_call({:cache_connection, %PostgresDatabase{} = db, conn}, _from, %State{} = state) do
    new_state = State.store_connection(state, db, conn)
    {:reply, :ok, new_state}
  end

  @impl GenServer
  def handle_cast({:invalidate_connection, %PostgresDatabase{} = db}, %State{} = state) do
    new_state = State.invalidate_connection(state, db)
    {:noreply, new_state}
  end

  @impl GenServer
  def handle_info({ref, {:start_result, result}}, %State{} = state) do
    Process.demonitor(ref, [:flush])

    {listeners, state} =
      case State.pop_start_task(state, ref) do
        {nil, _state} ->
          Logger.warning("[ConnectionCache] Start task not found", ref: ref)
          {[], state}

        {start_task, new_state} ->
          case result do
            {:ok, conn} ->
              new_state = State.store_connection(new_state, start_task.db, conn)
              {start_task.listeners, new_state}

            _error ->
              {start_task.listeners, new_state}
          end
      end

    Enum.each(listeners, fn from ->
      GenServer.reply(from, result)
    end)

    {:noreply, state}
  end

  # We'll receive EXIT messages from the connections we've cached.
  def handle_info({:EXIT, _pid, _reason}, %State{} = state) do
    {:noreply, state}
  end

  # This is from the GenServer.stop/1 call in State.stop_fn or from the start_fn crashing
  def handle_info({:DOWN, ref, :process, _pid, reason}, %State{} = state) do
    {listeners, state} =
      case State.pop_start_task(state, ref) do
        {nil, _state} ->
          # Must have been from the GenServer.stop/1 call in State.stop_fn
          {[], state}

        {start_task, new_state} ->
          Logger.warning("[ConnectionCache] Start task crashed", reason: reason)
          {start_task.listeners, new_state}
      end

    Enum.each(listeners, fn from ->
      GenServer.reply(
        from,
        {:error, Error.service(service: :customer_postgres, message: "Connection start function crashed")}
      )
    end)

    {:noreply, state}
  end

  @impl GenServer
  def terminate(_reason, %State{} = state) do
    _new_state = State.invalidate_all(state)
    :ok
  end
end
