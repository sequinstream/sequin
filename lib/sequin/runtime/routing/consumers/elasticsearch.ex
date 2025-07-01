defmodule Sequin.Runtime.Routing.Consumers.Elasticsearch do
  @moduledoc false
  use Sequin.Runtime.Routing.RoutedConsumer

  @primary_key false
  @derive {Jason.Encoder, only: [:index_name]}
  typed_embedded_schema do
    field :index_name, :string
  end

  def changeset(struct, params) do
    allowed_keys = [:index_name]

    struct
    |> cast(params, allowed_keys, empty_values: [])
    |> Routing.Helpers.validate_no_extra_keys(params, allowed_keys)
    |> validate_required([:index_name])
    |> validate_length(:index_name, min: 1, max: 1024)
  end

  def route(_action, _record, _changes, metadata) do
    %{index_name: "sequin.#{metadata.database.name}.#{metadata.table_schema}.#{metadata.table_name}"}
  end

  def route_consumer(%Sequin.Consumers.SinkConsumer{sink: sink}) do
    %{index_name: sink.index_name}
  end
end
