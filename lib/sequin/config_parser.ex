defmodule Sequin.ConfigParser do
  @moduledoc false
  require Logger

  def redis_config(env) do
    [socket_options: []]
    |> put_redis_url(env)
    |> put_redis_opts_ssl(env)
    |> put_redis_opts_ipv6(env)
  end

  defp put_redis_url(opts, %{"REDIS_URL" => url}) do
    Keyword.put(opts, :url, url)
  end

  defp put_redis_url(_opts, _env) do
    raise "REDIS_URL is not set. Please set the REDIS_URL env variable. Docs: #{doc_link(:redis)}"
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
              "[ConfigParser] Failed to get total memory bytes, using :infinity. (This can happen if Sequin is running on a non-Linux system)"
            )

            :infinity
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
end
