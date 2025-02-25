defmodule Sequin.Redis do
  @moduledoc false
  alias __MODULE__
  alias Sequin.Error
  alias Sequin.Error.ServiceError
  alias Sequin.Statsd

  require Logger

  @type command :: [any()]
  @type redis_value :: binary() | integer() | nil | [redis_value()]
  @type pipeline_return_value :: redis_value() | ServiceError.t()
  @type command_opt :: {:query_name, String.t()}

  @doc """
  :eredis_cluster_sup_sup has already been started elsewhere. To start nodes underneath it,
  we need to call the connect/3 function.
  connect/3 calls :eredis_cluster_sup_sup.start_child
  """
  def connect_cluster do
    {url, opts} = Keyword.pop!(config(), :url)
    %{host: host, port: port, userinfo: userinfo} = URI.parse(url)
    cluster_nodes = [{to_charlist(host), port}]

    # Parse username and password from userinfo
    opts =
      case userinfo do
        nil ->
          opts

        info ->
          {username, password} =
            case String.split(info, ":") do
              [user, pass] -> {user, pass}
              [pass] -> {nil, pass}
            end

          opts
          |> Keyword.put(:username, username)
          |> Keyword.put(:password, password)
      end

    # Start connections for each pool member
    for index <- 0..(pool_size() - 1) do
      :ok = :eredis_cluster.connect(connection(index), cluster_nodes, opts)
    end
  rescue
    error ->
      raise "Failed to connect to Redis: #{inspect(error)}"
  end

  @spec command(command(), [opt]) :: {:ok, redis_value()} | {:error, ServiceError.t()}
        when opt: command_opt()
  def command(command, opts \\ []) do
    maybe_time(command, opts[:query_name], fn ->
      res =
        connection()
        |> :eredis_cluster.q(command)
        |> parse_result()

      case res do
        {:ok, result} ->
          {:ok, result}

        {:error, :no_connection} ->
          {:error, Error.service(service: :redis, code: "no_connection", message: "No connection to Redis")}

        {:error, :timeout} ->
          {:error, Error.service(service: :redis, code: :timeout, message: "Timeout connecting to Redis")}

        {:error, error} when is_binary(error) or is_atom(error) ->
          Logger.error("Redis command failed: #{error}", error: error)
          {:error, Error.service(service: :redis, code: :command_failed, message: to_string(error))}
      end
    end)
  end

  @spec command!(command(), [opt]) :: redis_value()
        when opt: command_opt()
  def command!(command, opts \\ []) do
    maybe_time(command, opts[:query_name], fn ->
      res = connection() |> :eredis_cluster.q(command) |> parse_result()

      case res do
        {:ok, result} -> result
        {:error, error} when is_exception(error) -> raise error
        {:error, error} -> raise Error.service(service: :redis, code: :command_failed, message: error)
      end
    end)
  end

  @spec pipeline([command()], [opt]) :: {:ok, [pipeline_return_value()]} | {:error, ServiceError.t()}
        when opt: command_opt()
  def pipeline(commands, opts \\ []) do
    maybe_time(commands, opts[:query_name], fn ->
      case :eredis_cluster.q(connection(), commands) do
        results when is_list(results) ->
          # Convert eredis results to Redix-style results
          {:ok,
           Enum.map(results, fn
             {:ok, :undefined} ->
               nil

             {:ok, value} ->
               value

             {:error, error} when is_binary(error) ->
               Error.service(service: :redis, code: :command_failed, message: error)
           end)}

        {:error, :no_connection} ->
          {:error, Error.service(service: :redis, code: "no_connection", message: "No connection to Redis")}
      end
    end)
  end

  defp parse_result({:ok, :undefined}), do: {:ok, nil}

  defp parse_result({:ok, result}) when is_list(result) do
    {:ok,
     Enum.map(result, fn
       :undefined -> nil
       other -> other
     end)}
  end

  defp parse_result(result), do: result

  defp config do
    :sequin
    |> Application.get_env(__MODULE__, [])
    |> Sequin.Keyword.reject_nils()
  end

  defp connection(index \\ random_index()) do
    :"#{Redis}_#{index}"
  end

  defp random_index do
    Enum.random(0..(pool_size() - 1))
  end

  defp pool_size do
    :sequin |> Application.fetch_env!(Redis) |> Keyword.fetch!(:pool_size)
  end

  @sample_rate 1
  defp maybe_time([command_kind | _commands], query_name, fun) do
    query_name = query_name || "unnamed_query"
    {time_us, result} = :timer.tc(fun)

    if Enum.random(0..99) < @sample_rate do
      time_ms = time_us / 1000
      Statsd.timing("sequin.redis.#{command_kind}", time_ms, tags: %{query: query_name})
    end

    result
  end
end
