defmodule Sequin.Consumers.S2Sink do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  alias Sequin.Encrypted

  @derive {Jason.Encoder, only: [:endpoint_url, :stream]}
  @derive {Inspect, except: [:access_token]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:s2], default: :s2
    field :endpoint_url, :string
    field :stream, :string
    field :access_token, Encrypted.Field
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:endpoint_url, :stream, :access_token])
    |> validate_required([:endpoint_url, :stream, :access_token])
    |> validate_endpoint_url()
  end

  defp validate_endpoint_url(changeset) do
    changeset
    |> validate_change(:endpoint_url, fn :endpoint_url, url ->
      case URI.parse(url) do
        %URI{scheme: scheme, host: host} when not is_nil(scheme) and not is_nil(host) -> []
        _ -> [endpoint_url: "must be a valid URL"]
      end
    end)
    |> validate_length(:endpoint_url, max: 4096)
  end
end
