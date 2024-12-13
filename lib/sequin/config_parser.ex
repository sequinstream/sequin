defmodule Sequin.ConfigParser do
  @moduledoc false
  def redis_url(%{"REDIS_URL" => url}) do
    url
  end

  def redis_url(_env) do
    raise "REDIS_URL is not set. Please set the REDIS_URL env variable. Docs: #{doc_link(:redis)}"
  end

  def redis_opts(env) do
    [socket_opts: []]
    |> put_redis_opts_ssl(env)
    |> put_redis_opts_ipv6(env)
  end

  defp put_redis_opts_ssl(opts, %{"REDIS_SSL" => ssl}) do
    cond do
      ssl in ~w(true 1 verify-none) ->
        opts
        |> Keyword.update!(:socket_opts, fn opts -> opts ++ [verify: :verify_none] end)
        |> Keyword.put(:ssl, true)

      ssl in ~w(false 0) ->
        opts

      true ->
        raise "REDIS_SSL must be true, 1, verify-none, false, or 0. Docs: #{doc_link(:redis)}"
    end
  end

  defp put_redis_opts_ssl(opts, env) do
    url = redis_url(env)

    if String.starts_with?(url, "rediss://") do
      opts
      |> Keyword.put(:ssl, true)
      |> Keyword.update!(:socket_opts, fn opts -> opts ++ [verify: :verify_none] end)
    else
      opts
    end
  end

  defp put_redis_opts_ipv6(opts, %{"REDIS_IPV6" => ipv6}) do
    cond do
      ipv6 in ~w(true 1) ->
        Keyword.update!(opts, :socket_opts, fn opts -> opts ++ [:inet6] end)

      ipv6 in ~w(false 0) ->
        opts

      true ->
        raise "REDIS_IPV6 must be true, 1, false, or 0. Docs: #{doc_link(:redis)}"
    end
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
end
