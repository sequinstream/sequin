defmodule Sequin.Runtime.Routing.Consumers.Typesense do
  @moduledoc false
  use Sequin.Runtime.Routing.RoutedConsumer

  @primary_key false
  @derive {Jason.Encoder, only: [:collection_name]}
  typed_embedded_schema do
    field :collection_name, :string
  end

  def changeset(struct, params) do
    allowed_keys = [:collection_name]

    struct
    |> cast(params, allowed_keys, empty_values: [])
    |> Routing.Helpers.validate_no_extra_keys(params, allowed_keys)
    |> validate_required([:collection_name])
    |> validate_length(:collection_name, min: 1, max: 1024)
  end

  def route(_action, _record, _changes, metadata) do
    %{
      collection_name: "sequin.#{metadata.table_schema}.#{metadata.table_name}"
    }
  end

  def route_consumer(%Sequin.Consumers.SinkConsumer{sink: sink}) do
    %{collection_name: sink.collection_name}
  end
end
