defmodule Sequin.DebouncedLogger do
  @moduledoc """
  Roll-up / debounce wrapper around `Logger`.

  - First call for a `{level, dedupe_key}` is logged immediately.
  - Subsequent calls inside `debounce_interval_ms` are *suppressed* and counted.
  - When the timer fires, we emit **one** summary line
      "[Nx] original message"  (only if N > 0).

  No GenServer is used; everything is handled with an ETS table
  (`:debounced_logger_buckets`) plus `:timer.apply_after/4`.
  """

  @default_table :debounced_logger_buckets
  @levels [:debug, :info, :warning, :error]

  # ---------------------------------------------------------------------------
  # Public bootstrap
  # ---------------------------------------------------------------------------

  @doc """
  Create the ETS table.
  Call this once during application start (or rely on lazy creation).
  """
  @spec setup_ets(atom()) :: :ok
  def setup_ets(table_name \\ @default_table) do
    case :ets.info(table_name) do
      :undefined ->
        :ets.new(
          table_name,
          [:set, :named_table, :public, read_concurrency: true, write_concurrency: true]
        )

        :ok

      _info ->
        :ok
    end
  end

  # ---------------------------------------------------------------------------
  # Config struct
  # ---------------------------------------------------------------------------

  defmodule Config do
    @moduledoc false
    use TypedStruct

    typedstruct do
      field :dedupe_key, term(), enforce: true
      field :debounce_interval_ms, pos_integer(), default: 60_000
      field :table_name, atom()
    end
  end

  # ---------------------------------------------------------------------------
  # Public logging API – one function per level
  # ---------------------------------------------------------------------------

  @doc """
  Debounced `Logger.debug/2`.

  * `message` - string or iodata printed on first occurrence.
  * `config`  - `%DebouncedLogger.Config{}` (requires `dedupe_key`).
  * `metadata` - optional keyword list merged into `Logger.metadata/0`.
  """
  @spec debug(iodata(), %Config{}, Keyword.t()) :: :ok
  def debug(message, %Config{} = cfg, metadata \\ []) do
    log(:debug, message, cfg, metadata)
  end

  @doc """
  Debounced `Logger.info/2`.

  * `message` - string or iodata printed on first occurrence.
  * `config`  - `%DebouncedLogger.Config{}` (requires `dedupe_key`).
  * `metadata` - optional keyword list merged into `Logger.metadata/0`.
  """
  @spec info(iodata(), %Config{}, Keyword.t()) :: :ok
  def info(message, %Config{} = cfg, metadata \\ []) do
    log(:info, message, cfg, metadata)
  end

  @doc """
  Debounced `Logger.warning/2`.

  * `message` - string or iodata printed on first occurrence.
  * `config`  - `%DebouncedLogger.Config{}` (requires `dedupe_key`).
  * `metadata` - optional keyword list merged into `Logger.metadata/0`.
  """
  @spec warning(iodata(), %Config{}, Keyword.t()) :: :ok
  def warning(message, %Config{} = cfg, metadata \\ []) do
    log(:warning, message, cfg, metadata)
  end

  @doc """
  Debounced `Logger.error/2`.

  * `message` - string or iodata printed on first occurrence.
  * `config`  - `%DebouncedLogger.Config{}` (requires `dedupe_key`).
  * `metadata` - optional keyword list merged into `Logger.metadata/0`.
  """
  @spec error(iodata(), %Config{}, Keyword.t()) :: :ok
  def error(message, %Config{} = cfg, metadata \\ []) do
    log(:error, message, cfg, metadata)
  end

  # ---------------------------------------------------------------------------
  # Core implementation
  # ---------------------------------------------------------------------------

  @spec log(atom(), iodata(), %Config{}, Keyword.t()) :: :ok
  defp log(level, message, %Config{} = cfg, metadata) when level in @levels do
    key = {level, cfg.dedupe_key}
    merged_meta = Keyword.merge(Logger.metadata(), metadata)
    table_name = cfg.table_name || @default_table

    case :ets.lookup(table_name, key) do
      [] ->
        # First event - log immediately and start timer.
        Logger.bare_log(level, message, merged_meta)

        :ets.insert(table_name, {key, 0, IO.iodata_to_binary(message), merged_meta})

        # schedule flush; ignore returned tref (we don't cancel)
        _ =
          :timer.apply_after(cfg.debounce_interval_ms, __MODULE__, :flush_bucket, [level, cfg.dedupe_key, table_name])

        :ok

      [_existing] ->
        # Increment repeat count (element 2 in the tuple).
        :ets.update_counter(table_name, key, {2, 1})
        :ok
    end
  end

  # Called by :timer.apply_after/4
  @spec flush_bucket(atom(), term(), atom()) :: :ok
  def flush_bucket(level, dedupe_key, table_name \\ @default_table) when level in @levels do
    key = {level, dedupe_key}

    case :ets.take(table_name, key) do
      [{^key, 0, _msg, _meta}] ->
        # No repeats - do nothing.
        :ok

      [{^key, count, base_msg, meta}] when count > 0 ->
        summary = "[#{count}×] " <> base_msg
        Logger.bare_log(level, summary, meta)
        :ok

      _ ->
        :ok
    end
  end
end
