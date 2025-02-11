defmodule Sequin.Redis do
  @moduledoc false
  alias __MODULE__
  alias Sequin.Error
  alias Sequin.Error.ServiceError

  require Logger

  @type command :: [any()]
  @type redis_value :: binary() | integer() | nil | [redis_value()]
  @type pipeline_return_value :: redis_value() | ServiceError.t()

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

  @spec command(command()) :: {:ok, redis_value()} | {:error, ServiceError.t()}
  def command(command) do
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
  end

  @spec command!(command()) :: redis_value()
  def command!(command) do
    res = connection() |> :eredis_cluster.q(command) |> parse_result()

    case res do
      {:ok, result} -> result
      {:error, error} when is_exception(error) -> raise error
      {:error, error} -> raise Error.service(service: :redis, code: :command_failed, message: error)
    end
  end

  @spec pipeline([command()]) :: {:ok, [pipeline_return_value()]} | {:error, ServiceError.t()}
  def pipeline(commands) do
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
end
