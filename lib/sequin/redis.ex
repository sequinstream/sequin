defmodule Sequin.Redis do
  @moduledoc false
  @config Application.compile_env(:sequin, __MODULE__, [])

  def child_spec do
    url = Keyword.fetch!(@config, :url)
    opts = Keyword.get(@config, :opts, [])
    opts = [name: __MODULE__] ++ opts
    {Redix, {url, opts}}
  end

  @spec command(Redix.command(), keyword()) ::
          {:ok, Redix.Protocol.redis_value()}
          | {:error, atom() | Redix.Error.t() | Redix.ConnectionError.t()}
  def command(command, opts \\ []) do
    Redix.command(__MODULE__, command, opts)
  end

  @spec command!(Redix.command(), keyword()) :: Redix.Protocol.redis_value()
  def command!(command, opts \\ []) do
    Redix.command!(__MODULE__, command, opts)
  end

  @spec pipeline([Redix.command()], keyword()) ::
          {:ok, [Redix.Protocol.redis_value()]}
          | {:error, atom() | Redix.Error.t() | Redix.ConnectionError.t()}
  def pipeline(commands, opts \\ []) do
    Redix.pipeline(__MODULE__, commands, opts)
  end
end
