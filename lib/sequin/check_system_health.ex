defmodule Sequin.CheckSystemHealth do
  @moduledoc false
  alias Sequin.Error
  alias Sequin.Error.ValidationError
  alias Sequin.NetworkUtils
  alias Sequin.Redis
  alias Sequin.Repo

  require Logger

  def check do
    with {:ok, %Postgrex.Result{rows: [[1]]}} <- Repo.query("SELECT 1"),
         {:ok, "PONG"} <- Redis.command(["PING", "PONG"]) do
      :ok
    else
      {:error, %Error.ServiceError{service: :redis, code: "connection_error"} = error} ->
        redis_url = Application.get_env(:sequin, Sequin.Redis)[:url]
        %{host: redis_host, port: redis_port} = URI.parse(redis_url)

        with {:ok, ipv6} <- NetworkUtils.check_ipv6(redis_host),
             :ok <- NetworkUtils.test_tcp_reachability(redis_host, redis_port, ipv6) do
          # Not a network issue
          {:error,
           Error.service(
             service: :redis,
             message:
               "Error with system Redis. Can reach Redis on specified host and port via TCP, but can't execute queries. May be an auth issue.",
             details: error
           )}
        else
          {:error, %ValidationError{} = error} ->
            {:error, Error.service(service: :redis, message: Exception.message(error), code: "tcp_reachability_error")}
        end

      {:error, %Postgrex.Error{} = error} ->
        postgres_host = Repo.config()[:hostname] || "localhost"
        postgres_port = Repo.config()[:port] || 5432

        # Test Postgres reachability
        with {:ok, ipv6} <- NetworkUtils.check_ipv6(postgres_host),
             :ok <- NetworkUtils.test_tcp_reachability(postgres_host, postgres_port, ipv6) do
          # Not a network issue
          {:error,
           Error.service(
             service: :postgres,
             message:
               "Error with system Postgres. Can reach Postgres on specified host and port via TCP, but can't execute queries. May be an auth or SSL configuration issue.",
             details: error
           )}
        else
          {:error, %ValidationError{} = error} ->
            {:error, Error.service(service: :postgres, message: Exception.message(error))}
        end

      {:error, error} ->
        Logger.error("Unknown error while checking system health: #{Exception.message(error)}")
        {:error, Error.service(service: :sequin, message: Exception.message(error), details: error)}
    end
  end
end
