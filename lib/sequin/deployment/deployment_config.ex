defmodule Sequin.Deployment.DeploymentConfig do
  @moduledoc """
  Schema for storing key/value deployment configuration.

  Only used on self-hosted Sequin, for dynamic configuration settings not covered by eg env variables.
  """
  use Sequin.ConfigSchema

  import Ecto.Changeset
  import Ecto.Query, only: [from: 2]

  @derive {Jason.Encoder, only: [:key, :value, :inserted_at, :updated_at]}

  @primary_key {:key, :string, []}
  typed_schema "deployment_config" do
    field :value, :map

    timestamps()
  end

  @doc """
  Changeset function for the DeploymentConfig schema.
  """
  def changeset(config, attrs) do
    config
    |> cast(attrs, [:key, :value])
    |> validate_length(:key, max: 255)
    |> unique_constraint([:key])
  end

  def where_key(query \\ base_query(), key) do
    from(c in query, where: c.key == ^key)
  end

  defp base_query(query \\ __MODULE__) do
    from(c in query, as: :deployment_config)
  end
end
