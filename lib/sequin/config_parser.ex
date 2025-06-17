defmodule Sequin.ConfigParser do
  @moduledoc false
  require Logger

  @secret_generation_docs_link "https://sequinstream.com/docs/reference/configuration#secret-generation"

  def default_workers_per_sink(env) do
    case env["DEFAULT_WORKERS_PER_SINK"] do
      nil ->
        nil

      workers_str ->
        case Integer.parse(workers_str) do
          {workers, ""} when workers >= 1 ->
            workers

          _ ->
            raise ArgumentError, "DEFAULT_WORKERS_PER_SINK must be an integer >= 1. Got: #{inspect(workers_str)}"
        end
    end
  end

  def redis_config(env) do
    [socket_options: [], pool_size: parse_pool_size(env)]
    |> put_redis_url(env)
    |> put_redis_opts_ssl(env)
    |> put_redis_opts_ipv6(env)
  end

  @valid_log_levels [:error, :warning, :notice, :info, :debug]
  def log_level(env, default_level) when default_level in @valid_log_levels do
    env
    |> Map.get("LOG_LEVEL", to_string(default_level))
    |> String.downcase()
    |> String.to_atom()
    |> validate_log_level(default_level)
  end

  def secret_key_base(env) do
    secret = Map.get(env, "SECRET_KEY_BASE")
    validate_required(secret, "SECRET_KEY_BASE")
    validate_length_gte(secret, "SECRET_KEY_BASE", 64)
    secret
  end

  def vault_key(env) do
    secret = Map.get(env, "VAULT_KEY")
    validate_required(secret, "VAULT_KEY")
    validate_length_gte(secret, "VAULT_KEY", 32)
    validate_base64(secret, "VAULT_KEY")
    secret
  end

  defp validate_required(env_var, env_name) do
    if is_nil(env_var) do
      raise ArgumentError, """
      Environment variable #{env_name} is not set.
      """
    end
  end

  defp validate_length_gte(env_var, env_name, expected_length) when is_binary(env_var) do
    if byte_size(env_var) < expected_length do
      raise ArgumentError, """
      Environment variable #{env_name} is too short.

      Expected at least #{expected_length} bytes. Got #{byte_size(env_var)} bytes.
      """
    end
  end

  defp validate_base64(secret, secret_name) do
    case Base.decode64(secret) do
      {:ok, _} ->
        :ok

      :error ->
        raise ArgumentError, """
        Secret #{secret_name} is not valid base64.

        Got: #{inspect(secret)}

        See: #{@secret_generation_docs_link}
        """
    end
  end

  defp validate_log_level(level, _default_level) when level in @valid_log_levels, do: level

  defp validate_log_level(level, default_level) do
    Logger.warning("[ConfigParser] Invalid log level: #{inspect(level)}. Using default #{inspect(default_level)} level.")

    default_level
  end

  defp put_redis_url(opts, %{"REDIS_URL" => url}) do
    url = url |> ensure_redis_scheme() |> ensure_redis_port()
    Keyword.put(opts, :url, url)
  end

  defp put_redis_url(_opts, _env) do
    raise ArgumentError, "REDIS_URL is not set. Please set the REDIS_URL env variable. Docs: #{doc_link(:redis)}"
  end

  defp ensure_redis_scheme(url) do
    uri = URI.parse(url)

    if is_nil(uri.scheme) do
      # Prepend redis:// if no scheme is present
      "redis://" <> url
    else
      url
    end
  end

  defp ensure_redis_port(url) do
    uri = URI.parse(url)

    if is_nil(uri.port) do
      # Default to port 6379 if port is missing
      URI.to_string(%{uri | port: 6379})
    else
      url
    end
  end

  defp put_redis_opts_ssl(opts, %{"REDIS_SSL" => ssl}) do
    cond do
      ssl in ~w(true 1 verify-none) ->
        Keyword.put(opts, :tls, verify: :verify_none)

      ssl in ~w(false 0) ->
        # `nil` is important to override any settings set in config.exs
        Keyword.put(opts, :tls, nil)

      true ->
        raise ArgumentError, "REDIS_SSL must be true, 1, verify-none, false, or 0. Docs: #{doc_link(:redis)}"
    end
  end

  defp put_redis_opts_ssl(opts, _env) do
    url = Keyword.fetch!(opts, :url)

    if String.starts_with?(url, "rediss://") do
      Keyword.put(opts, :tls, verify: :verify_none)
    else
      # `nil` is important to override any settings set in config.exs
      Keyword.put(opts, :tls, nil)
    end
  end

  defp put_redis_opts_ipv6(opts, %{"REDIS_IPV6" => _ipv6}) do
    # :eredis auto-detects ipv6
    # We can remove this flag from our docs
    opts
  end

  defp put_redis_opts_ipv6(opts, _env) do
    opts
  end

  @config_doc "https://sequinstream.com/docs/reference/configuration"
  defp doc_link(section) do
    case section do
      :redis -> "#{@config_doc}#redis-configuration"
    end
  end

  def max_memory_bytes(env) do
    buffer_percent = parse_buffer_percent(env)

    case env["MAX_MEMORY_MB"] do
      nil ->
        # Use system memory with buffer
        case Sequin.System.total_memory_bytes() do
          {:ok, total_bytes} ->
            apply_buffer(total_bytes, buffer_percent)

          {:error, _} ->
            Logger.error(
              "[ConfigParser] Failed to get total memory bytes, using very high limit (100GB). (This can happen if Sequin is running on a non-Linux system. See \"Configuration\" reference for more details.)"
            )

            Sequin.Size.gb(100)
        end

      mb_str ->
        # Use explicit limit with buffer
        mb_str
        |> String.to_integer()
        |> Sequin.Size.mb()
        |> apply_buffer(buffer_percent)
    end
  end

  defp parse_buffer_percent(env) do
    env
    |> Map.get("MEMORY_BUFFER_PERCENT", "20")
    |> String.to_integer()
    |> Kernel./(100)
  end

  defp apply_buffer(bytes, buffer_percent) do
    trunc(bytes * (1 - buffer_percent))
  end

  defp parse_pool_size(env) do
    pool_size = Map.get(env, "REDIS_POOL_SIZE", "5")

    case Integer.parse(pool_size) do
      {size, ""} when size > 0 ->
        size

      _ ->
        raise ArgumentError, "REDIS_POOL_SIZE must be a positive integer. Got: #{inspect(pool_size)}."
    end
  end

  @doc """
  Returns the value of REPLICATION_FLUSH_MAX_ACCUMULATED_BYTES as a positive integer, or nil if not set/invalid.
  """
  def replication_flush_max_accumulated_bytes(env) do
    parse_positive_int(env["REPLICATION_FLUSH_MAX_ACCUMULATED_BYTES"], "REPLICATION_FLUSH_MAX_ACCUMULATED_BYTES")
  end

  @doc """
  Returns the value of REPLICATION_FLUSH_MAX_ACCUMULATED_MESSAGES as a positive integer, or nil if not set/invalid.
  """
  def replication_flush_max_accumulated_messages(env) do
    parse_positive_int(env["REPLICATION_FLUSH_MAX_ACCUMULATED_MESSAGES"], "REPLICATION_FLUSH_MAX_ACCUMULATED_MESSAGES")
  end

  @doc """
  Returns the value of REPLICATION_FLUSH_MAX_ACCUMULATED_TIME_MS as a positive integer, or nil if not set/invalid.
  """
  def replication_flush_max_accumulated_time_ms(env) do
    parse_positive_int(env["REPLICATION_FLUSH_MAX_ACCUMULATED_TIME_MS"], "REPLICATION_FLUSH_MAX_ACCUMULATED_TIME_MS")
  end

  # Parse a positive integer from an environment variable.
  # Returns the integer if valid, or nil if the value is invalid or not positive (and raise: false).
  #
  # ## Options
  # * `:raise` - If true, raises an ArgumentError when the value is invalid. Defaults to false.
  #
  defp parse_positive_int(env_value, env_name, opts \\ []) do
    raise_error = Keyword.get(opts, :raise, false)

    case env_value do
      nil ->
        nil

      value ->
        case Integer.parse(value) do
          {int, ""} when int > 0 ->
            int

          _ ->
            if raise_error do
              raise ArgumentError, "#{env_name} must be a positive integer. Got: #{inspect(value)}."
            else
              Logger.warning("#{env_name} must be a positive integer. Got: #{inspect(value)}. Ignoring.")
              nil
            end
        end
    end
  end
end
