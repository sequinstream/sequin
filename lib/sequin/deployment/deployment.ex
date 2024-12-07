defmodule Sequin.Deployment do
  @moduledoc """
  Only used on self-hosted Sequin, for dynamic configuration settings not covered by eg env variables.
  """
  alias Sequin.Deployment.DeploymentConfig
  alias Sequin.Repo

  def config(key) do
    case :persistent_term.get({__MODULE__, key}, :not_found) do
      :not_found ->
        value = DeploymentConfig |> DeploymentConfig.where_key(key) |> Repo.one()
        :persistent_term.put({__MODULE__, key}, value)

        value

      value ->
        value
    end
  end

  def set_config(key, value) do
    result =
      %DeploymentConfig{}
      |> DeploymentConfig.changeset(%{key: key, value: value})
      |> Repo.insert(on_conflict: {:replace, [:value]}, conflict_target: [:key])

    # Bust the cache after successful update
    if match?({:ok, _}, result), do: :persistent_term.erase({__MODULE__, key})

    result
  end
end
