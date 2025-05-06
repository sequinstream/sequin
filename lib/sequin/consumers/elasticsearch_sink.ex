defmodule Sequin.Consumers.ElasticsearchSink do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  @derive {Jason.Encoder, only: [:endpoint_url, :index_name, :auth_type]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:elasticsearch], default: :elasticsearch
    field :endpoint_url, :string
    field :index_name, :string
    field :auth_type, Ecto.Enum, values: [:api_key, :basic, :bearer], default: :api_key
    field :auth_value, Sequin.Encrypted.Binary
    field :batch_size, :integer, default: 100
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:endpoint_url, :index_name, :auth_type, :auth_value, :batch_size])
    |> validate_required([:endpoint_url, :index_name, :auth_type, :auth_value])
    |> validate_endpoint_url()
    |> validate_length(:index_name, max: 1024)
    |> validate_number(:batch_size, greater_than: 0, less_than_or_equal_to: 10_000)
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
