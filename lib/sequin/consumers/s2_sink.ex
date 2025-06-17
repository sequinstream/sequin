defmodule Sequin.Consumers.S2Sink do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias Sequin.Encrypted

  @derive {Jason.Encoder, only: [:basin, :stream]}
  @derive {Inspect, except: [:access_token]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:s2], default: :s2
    field :basin, :string
    field :stream, :string
    field :access_token, Encrypted.Field
  end

  def endpoint_url(%__MODULE__{basin: basin}) do
    "https://#{basin}.b.aws.s2.dev/v1/"
  end

  def dashboard_url(%__MODULE__{basin: basin}) do
    "https://s2.dev/dashboard/basins/#{basin}"
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:basin, :stream, :access_token])
    |> validate_required([:basin, :stream, :access_token])
    |> validate_basin()
  end

  defp validate_basin(changeset) do
    changeset
    |> validate_change(:basin, fn :basin, basin ->
      case basin do
        basin when is_binary(basin) and basin != "" -> []
        _ -> [basin: "must be a non-empty string"]
      end
    end)
    |> validate_length(:basin, min: 8, max: 48)
    |> validate_format(:basin, ~r/^[a-z0-9]([a-z0-9-]*[a-z0-9])?$/,
      message: "can only include lowercase letters, numbers, and hyphens. It cannot begin or end with a hyphen."
    )
  end
end
