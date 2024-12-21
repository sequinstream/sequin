defmodule Sequin.Redis do
  @moduledoc false
  alias Sequin.Error
  alias Sequin.Error.ServiceError

  require Logger

  @type command :: [any()]
  @type return_value :: nil | binary() | [binary() | nonempty_list()]
  @type pipeline_return_value :: nil | binary() | [binary() | nonempty_list()] | ServiceError.t()

  @config Application.compile_env(:sequin, __MODULE__, [])

  def child_spec do
    %{host: host, port: port} = URI.parse(Keyword.fetch!(@config, :url))
    opts = Keyword.get(@config, :opts, [])
    opts = [name: {:local, __MODULE__}, host: to_charlist(host), port: port] ++ opts

    %{
      id: __MODULE__,
      type: :worker,
      restart: :permanent,
      start: {:eredis, :start_link, [opts]}
    }
  end

  @spec command(command()) :: {:ok, return_value()} | {:error, ServiceError.t()}
  def command(command) do
    res =
      __MODULE__
      |> :eredis.q(command)
      |> parse_result()

    case res do
      {:ok, result} ->
        {:ok, result}

      {:error, :no_connection} ->
        {:error, Error.service(service: :redis, code: :no_connection, message: "No connection to Redis")}

      {:error, error} when is_binary(error) ->
        Logger.error("Redis command failed: #{error}", error: error)
        {:error, Error.service(service: :redis, code: :command_failed, message: error)}
    end
  end

  @spec command!(command()) :: return_value()
  def command!(command) do
    res = __MODULE__ |> :eredis.q(command) |> parse_result()

    case res do
      {:ok, result} -> result
      {:error, error} -> raise error
    end
  end

  @spec pipeline([command()]) :: {:ok, [pipeline_return_value()]} | {:error, ServiceError.t()}
  def pipeline(commands) do
    case :eredis.qp(__MODULE__, commands) do
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
        {:error, Error.service(service: :redis, code: :no_connection, message: "No connection to Redis")}
    end
  end

  defp parse_result({:ok, :undefined}), do: {:ok, nil}
  defp parse_result(result), do: result
end
