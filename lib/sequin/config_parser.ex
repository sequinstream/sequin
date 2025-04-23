defmodule Sequin.ConfigParser do
  @moduledoc false
  require Logger

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

  defp validate_log_level(level, _default_level) when level in @valid_log_levels, do: level

  defp validate_log_level(level, default_level) do
    Logger.warning("[ConfigParser] Invalid log level: #{inspect(level)}. Using default #{inspect(default_level)} level.")
    default_level
  end

  defp put_redis_url(opts, %{"REDIS_URL" => url}) do
    url = ensure_redis_port(url)
    Keyword.put(opts, :url, url)
  end

  defp put_redis_url(_opts, _env) do
    raise "REDIS_URL is not set. Please set the REDIS_URL env variable. Docs: #{doc_link(:redis)}"
  end

  defp ensure_redis_port(url) do
    uri = URI.parse(url)

    if uri.port == nil do
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
        raise "REDIS_SSL must be true, 1, verify-none, false, or 0. Docs: #{doc_link(:redis)}"
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

            100 * 1024 * 1024 * 1024
        end

      mb_str ->
        # Use explicit limit with buffer
        mb_str
        |> String.to_integer()
        |> Kernel.*(1024 * 1024)
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
        raise "REDIS_POOL_SIZE must be a positive integer. Got: #{inspect(pool_size)}."
    end
  end
end
