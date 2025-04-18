defmodule Sequin.Consumers.TypesenseSink do
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  @import_actions [:create, :upsert, :update, :emplace]

  @derive {Jason.Encoder, only: [:endpoint_url, :collection_name, :import_action]}
  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:typesense], default: :typesense
    field :endpoint_url, :string
    field :collection_name, :string
    field :api_key, Sequin.Encrypted.Binary
    field :import_action, Ecto.Enum, values: @import_actions, default: :emplace
    field :batch_size, :integer, default: 40
    field :timeout_seconds, :integer, default: 5
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [:endpoint_url, :collection_name, :api_key, :import_action, :batch_size, :timeout_seconds])
    |> validate_required([:endpoint_url, :collection_name, :api_key])
    |> validate_endpoint_url()
    |> validate_length(:collection_name, max: 1024)
    |> validate_number(:batch_size, greater_than: 0, less_than_or_equal_to: 10_000)
    |> validate_number(:timeout_seconds, greater_than: 0, less_than_or_equal_to: 300)
  end

  defp validate_endpoint_url(changeset) do
    changeset
    |> validate_change(:endpoint_url, fn :endpoint_url, url ->
      case URI.parse(url) do
        %URI{} ->
          []

        _ ->
          [endpoint_url: "must be a valid URL"]
      end
    end)
    |> validate_length(:endpoint_url, max: 4096)
  end

  def client_params(%__MODULE__{} = me) do
    [url: me.endpoint_url,
     api_key: me.api_key,
     timeout_seconds: me.timeout_seconds
    ]
  end
end
