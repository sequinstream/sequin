defmodule Sequin.MutexOwner do
  @moduledoc """
  This GenServer boots up and tries to acquire a mutex. When it does, it calls the `on_acquired` callback (supplied on boot.)

  If it ever loses the mutex (unexpected - it should be touching the mutex before it expires), it crashes.

  If Redis becomes temporarily unreachable while holding the mutex, the MutexOwner will retry
  with exponential backoff indefinitely (capped at 1 hour) rather than crashing. This handles
  Redis/Dragonfly/KeyDB restarts without cascading failures through MutexedSupervisor.
  When Redis comes back, the MutexOwner re-acquires the mutex and resumes normal operation.
  """
  use GenStateMachine

  alias Sequin.Mutex

  require Logger

  # Retry backoff caps at 1 hour
  @max_retry_interval to_timeout(hour: 1)

  defmodule State do
    @moduledoc """
    lock_expiry - how long to hold the mutex for when acquired
    mutex_key/mutex_token - see Sequin.Mutex
    on_acquired - callback that is called when the mutex is acquired
    consecutive_redis_errors - count of consecutive Redis errors while holding mutex
    """
    use TypedStruct

    typedstruct do
      field :lock_expiry, non_neg_integer(), default: to_timeout(second: 5)
      field :mutex_key, String.t(), required: true
      field :mutex_token, String.t(), required: true
      field :on_acquired, (-> any()), required: true
      field :last_emitted_passive_log, DateTime.t(), default: ~U[2000-01-01 00:00:00Z]
      field :consecutive_redis_errors, non_neg_integer(), default: 0
    end

    def new(opts) do
      opts = Keyword.put(opts, :mutex_token, UUID.uuid4())

      struct!(__MODULE__, opts)
    end
  end

  @type opt :: {:name, atom()} | {:on_acquired, (-> any())} | {:lock_expiry, non_neg_integer()}
  @spec start_link(list(opt)) :: GenStateMachine.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop!(opts, :name)
    GenStateMachine.start_link(__MODULE__, opts, name: name)
  end

  @impl GenStateMachine
  def init(opts) do
    Logger.metadata(sha: Application.get_env(:sequin, :sha), mutex_key: Keyword.fetch!(opts, :mutex_key))

    actions = [
      {{:timeout, :acquire_mutex}, 0, nil}
    ]

    state = State.new(opts)

    {:ok, :await_mutex, state, actions}
  end

  @impl GenStateMachine
  def handle_event({:timeout, :keep_mutex}, _evt, :has_mutex, data) do
    case acquire_mutex(data) do
      :ok ->
        {:keep_state, %{data | consecutive_redis_errors: 0}, [keep_timeout(data.lock_expiry)]}

      {:error, :mutex_taken} ->
        Logger.error("MutexOwner lost its mutex.")
        {:stop, {:shutdown, :lost_mutex}}

      :error ->
        errors = data.consecutive_redis_errors + 1
        # Exponential backoff: lock_expiry * 2^errors, capped at 1 hour
        retry_interval = min(data.lock_expiry * Integer.pow(2, errors), @max_retry_interval)

        Logger.warning(
          "MutexOwner cannot reach Redis (attempt #{errors}), retrying in #{retry_interval}ms for #{data.mutex_key}."
        )

        {:keep_state, %{data | consecutive_redis_errors: errors}, [{{:timeout, :keep_mutex}, retry_interval, nil}]}
    end
  end

  def handle_event({:timeout, :acquire_mutex}, _evt, :await_mutex, data) do
    case acquire_mutex(data) do
      :ok ->
        # just acquired lock
        Logger.info("MutexOwner just acquired mutex for #{data.mutex_key}")
        data.on_acquired.()

        actions = [
          keep_timeout(data.lock_expiry)
        ]

        {:next_state, :has_mutex, data, actions}

      {:error, :mutex_taken} ->
        reattempt_timeout = round(data.lock_expiry / 2)

        actions = [
          {{:timeout, :acquire_mutex}, reattempt_timeout, nil}
        ]

        if env() != :test and DateTime.diff(DateTime.utc_now(), data.last_emitted_passive_log, :minute) > 1 do
          Logger.info("Running in passive mode, another instance is holding active mutex.")
          {:keep_state, %{data | last_emitted_passive_log: DateTime.utc_now()}, actions}
        else
          {:keep_state_and_data, actions}
        end

      :error ->
        actions = [
          {{:timeout, :acquire_mutex}, data.lock_expiry, nil}
        ]

        {:keep_state_and_data, actions}
    end
  end

  @impl GenStateMachine
  def terminate(_reason, :has_mutex, %State{} = data) do
    Logger.info("MutexOwner terminating, releasing mutex")

    Mutex.release(data.mutex_key, data.mutex_token)
  end

  @impl GenStateMachine
  def terminate(_reason, _state, _), do: :ok

  defp keep_timeout(lock_expiry) do
    keep_at = round(lock_expiry * 0.80)
    {{:timeout, :keep_mutex}, keep_at, nil}
  end

  defp acquire_mutex(%State{} = data) do
    Mutex.acquire_or_touch(data.mutex_key, data.mutex_token, data.lock_expiry)
  end

  defp env do
    Application.get_env(:sequin, :env)
  end
end
