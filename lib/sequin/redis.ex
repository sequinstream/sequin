defmodule Sequin.Redis do
  @moduledoc false
  alias Sequin.Error

  def child_spec do
    url = Keyword.fetch!(config(), :url)
    opts = Keyword.get(config(), :opts, [])
    opts = [name: __MODULE__] ++ opts
    {Redix, {url, opts}}
  end

  @spec command(Redix.command(), keyword()) :: {:ok, Redix.Protocol.redis_value()} | {:error, Error.t()}
  def command(command, opts \\ []) do
    case Redix.command(__MODULE__, command, opts) do
      {:ok, value} -> {:ok, value}
      {:error, error} -> {:error, to_sequin_error(error)}
    end
  end

  @spec command!(Redix.command(), keyword()) :: Redix.Protocol.redis_value()
  def command!(command, opts \\ []) do
    case command(command, opts) do
      {:ok, value} -> value
      {:error, error} -> raise error
    end
  end

  @spec pipeline([Redix.command()], keyword()) :: {:ok, [Redix.Protocol.redis_value()]} | {:error, Error.t()}
  def pipeline(commands, opts \\ []) do
    case Redix.pipeline(__MODULE__, commands, opts) do
      {:ok, values} -> {:ok, values}
      {:error, error} -> {:error, to_sequin_error(error)}
    end
  end

  defp to_sequin_error(%Redix.ConnectionError{} = error) do
    Error.service(
      service: :redis,
      message: "Redis connection error: #{Exception.message(error)}",
      code: "connection_error"
    )
  end

  defp to_sequin_error(%Redix.Error{} = error) do
    Error.service(
      service: :redis,
      message: "Redis error: #{Exception.message(error)}",
      code: "command_error"
    )
  end

  defp to_sequin_error(error) when is_atom(error) do
    Error.service(
      service: :redis,
      message: "Redis error: #{error}",
      code: "command_error"
    )
  end

  defp config, do: Application.get_env(:sequin, __MODULE__, [])
end
