defmodule Sequin.Config do
  @moduledoc """
  Behaviour for application configuration access.
  """

  @doc """
  Gets the self-hosted environment configuration.
  """
  @callback self_hosted?() :: boolean()
  def self_hosted? do
    impl().self_hosted?()
  end

  defp impl do
    Application.get_env(:sequin, :config_module, Sequin.ApplicationConfig)
  end
end

defmodule Sequin.ApplicationConfig do
  @moduledoc """
  Application configuration utilities.
  """

  @behaviour Sequin.Config

  @doc """
  Returns true if the application is running in self-hosted mode.
  """
  @impl true
  @spec self_hosted?() :: boolean()
  def self_hosted? do
    Application.get_env(:sequin, :self_hosted, false)
  end
end
