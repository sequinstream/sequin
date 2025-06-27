defmodule Sequin.Consumers.TypesenseSink do
  @moduledoc false
  use Ecto.Schema
  use TypedEctoSchema

  import Ecto.Changeset

  @derive {Jason.Encoder, only: [:endpoint_url, :collection_name]}
  @derive {Inspect, except: [:api_key]}

  @primary_key false
  typed_embedded_schema do
    field :type, Ecto.Enum, values: [:typesense], default: :typesense
    field :endpoint_url, :string
    field :collection_name, :string
    field :api_key, Sequin.Encrypted.Binary
    field :batch_size, :integer, default: 100
    field :timeout_seconds, :integer, default: 5
    field :routing_mode, Ecto.Enum, values: [:dynamic, :static]
  end

  def changeset(struct, params) do
    struct
    |> cast(params, [
      :endpoint_url,
      :collection_name,
      :api_key,
      :batch_size,
      :timeout_seconds,
      :routing_mode
    ])
    |> validate_required([:endpoint_url, :api_key])
    |> validate_endpoint_url()
    |> validate_length(:collection_name, max: 1024)
    |> validate_number(:batch_size, greater_than: 0, less_than_or_equal_to: 10_000)
    |> validate_number(:timeout_seconds, greater_than: 0, less_than_or_equal_to: 300)
    |> validate_routing()
  end

  defp validate_endpoint_url(changeset) do
    changeset
    |> validate_change(:endpoint_url, fn :endpoint_url, url ->
      case URI.parse(url) do
        %URI{scheme: nil} ->
          [endpoint_url: "must include a scheme, ie. https://"]

        %URI{scheme: scheme} when scheme not in ["http", "https"] ->
          [endpoint_url: "must include a valid scheme, ie. http or https"]

        %URI{host: host} when is_nil(host) or host == "" ->
          [endpoint_url: "must include a host"]

        %URI{query: query} when not is_nil(query) ->
          [endpoint_url: "must not include query params, found: #{query}"]

        %URI{fragment: fragment} when not is_nil(fragment) ->
          [endpoint_url: "must not include a fragment, found: #{fragment}"]

        _ ->
          []
      end
    end)
    |> validate_length(:endpoint_url, max: 4096)
  end

  defp validate_routing(changeset) do
    routing_mode = get_field(changeset, :routing_mode)

    cond do
      routing_mode == :dynamic ->
        put_change(changeset, :collection_name, nil)

      routing_mode == :static ->
        validate_required(changeset, [:collection_name])

      true ->
        add_error(changeset, :routing_mode, "is required")
    end
  end

  def client_params(%__MODULE__{} = sink) do
    [url: sink.endpoint_url, api_key: sink.api_key, timeout_seconds: sink.timeout_seconds]
  end
end
