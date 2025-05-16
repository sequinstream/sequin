defmodule Sequin.Redis do
  @moduledoc false
  alias __MODULE__
  alias Sequin.Error
  alias Sequin.Error.ServiceError
  alias Sequin.Redis.RedisClient
  alias Sequin.Statsd

  require Logger

  @type command :: [any()]
  @type redis_value :: binary() | integer() | nil | [redis_value()]
  @type pipeline_return_value :: redis_value() | ServiceError.t()
  @type command_opt :: {:query_name, String.t()}

  defmodule ClusterClient do
    @moduledoc false
    def connect(index, %{host: host, port: port} = opts) do
      opts =
        opts
        |> Map.drop([:host, :port, :database])
        |> Keyword.new()

      cluster_nodes = [{host, port}]
      :ok = :eredis_cluster.connect(Sequin.Redis.connection(index), cluster_nodes, opts)
    end

    def q(connection, command) do
      :eredis_cluster.q(connection, command)
    end

    # Redis cluster only supports pipelines where all keys map to the same node
    # We can guarantee this throughout the system by using `hash tags`
    # ie. `my-key:{some-hash-value}`
    # we have not done this yet! so we hack it here with Enum.map
    # but: this raise was left as a warning
    def qp(connection, commands) do
      if env() == :prod or length(commands) <= 3 do
        Enum.map(commands, &q(connection, &1))
      else
        raise "Redis pipeline length must be <= 3. Received #{length(commands)} commands"
      end
    end

    defp env do
      Application.get_env(:sequin, :env)
    end
  end

  defmodule Client do
    @moduledoc false
    def connect(index, opts) do
      opts =
        opts
        |> Keyword.new()
        |> Keyword.put(:name, {:local, Sequin.Redis.connection(index)})
        |> Keyword.delete(:pool_size)

      {:ok, _pid} = :eredis.start_link(opts)
      :ok
    end

    def q(connection, command) do
      :eredis.q(connection, command)
    end

    def qp(connection, commands) do
      :eredis.qp(connection, commands)
    end
  end

  @doc """
  :eredis_cluster_sup_sup has already been started elsewhere. To start nodes underneath it,
  we need to call the connect/3 function.
  connect/3 calls :eredis_cluster_sup_sup.start_child
  """
  def connect_cluster do
    :ok = set_redis_client()
    opts = parse_redis_connection_opts()

    # Start connections for each pool member
    for index <- 0..(pool_size() - 1) do
      redis_client().connect(index, opts)
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
        |> redis_client().q(command)
        |> parse_result()

      case res do
        {:ok, result} ->
          {:ok, result}

        {:error, :no_connection} ->
          {:error,
           Error.service(
             service: :redis,
             code: "no_connection",
             message: "No connection to Redis"
           )}

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
      res = connection() |> redis_client().q(command) |> parse_result()

      case res do
        {:ok, result} ->
          result

        {:error, error} ->
          raise Error.service(service: :redis, code: :command_failed, message: error)
      end
    end)
  end

  @spec pipeline([command()], [opt]) ::
          {:ok, [pipeline_return_value()]} | {:error, ServiceError.t()}
        when opt: command_opt()
  def pipeline(commands, opts \\ []) do
    maybe_time(commands, opts[:query_name], fn ->
      case redis_client().qp(connection(), commands) do
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
          {:error,
           Error.service(
             service: :redis,
             code: "no_connection",
             message: "No connection to Redis"
           )}
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

  def connection(index \\ random_index()) do
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
    command_kind =
      if is_list(command_kind), do: "pipeline-#{List.first(command_kind)}", else: command_kind

    query_name = query_name || "unnamed_query"
    {time_ms, result} = :timer.tc(fun, :millisecond)

    if Enum.random(0..99) < @sample_rate do
      Statsd.timing("sequin.redis", time_ms, tags: %{query: query_name, command_kind: command_kind})
    end

    result
  end

  defp parse_redis_connection_opts do
    {url, opts} = Keyword.pop!(config(), :url)
    opts = Map.new(opts)

    %{host: host, port: port, userinfo: userinfo, path: path} = URI.parse(url)
    opts = Map.merge(opts, %{host: to_charlist(host), port: port})

    opts =
      case path do
        "/" <> database -> Map.put(opts, :database, String.to_integer(database))
        _ -> Map.put(opts, :database, 0)
      end

    # Parse username and password from userinfo
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
        |> Map.put(:username, username)
        |> Map.put(:password, password)
    end
  end

  defp set_redis_client do
    case parse_redis_connection_opts() do
      %{database: 0} ->
        Application.put_env(:sequin, RedisClient, ClusterClient)

      %{database: database} when database > 0 and database < 16 ->
        Application.put_env(:sequin, RedisClient, Client)
    end

    :ok
  end

  defp redis_client do
    Application.get_env(:sequin, RedisClient)
  end
end
