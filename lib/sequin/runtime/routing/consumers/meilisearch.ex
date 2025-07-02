defmodule Sequin.Runtime.Routing.Consumers.Meilisearch do
  @moduledoc false
  use Sequin.Runtime.Routing.RoutedConsumer

  @primary_key false
  @derive {Jason.Encoder, only: [:index_name]}
  typed_embedded_schema do
    field :action, Ecto.Enum, values: [:index, :delete]
    field :index_name, :string
  end

  def changeset(struct, params) do
    allowed_keys = [:action, :index_name]

    struct
    |> cast(params, allowed_keys, empty_values: [])
    |> Routing.Helpers.validate_no_extra_keys(params, allowed_keys)
    |> validate_required([:action, :index_name])
    |> validate_inclusion(:action, [:index, :delete])
    |> validate_length(:index_name, min: 1, max: 1024)
  end

  def route(action, _record, _changes, metadata) do
    meilisearch_action =
      case action do
        "insert" -> :index
        "update" -> :index
        "delete" -> :delete
        "read" -> :index
      end

    %{
      action: meilisearch_action,
      index_name: "sequin.#{metadata.table_schema}.#{metadata.table_name}"
    }
  end

  def route_consumer(%Sequin.Consumers.SinkConsumer{sink: sink}) do
    %{index_name: sink.index_name}
  end
end
