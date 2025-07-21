defmodule Sequin.Runtime.Routing.Consumers.Meilisearch do
  @moduledoc false
  use Sequin.Runtime.Routing.RoutedConsumer

  @primary_key false
  @derive {Jason.Encoder, only: [:index_name, :filter, :function, :context]}
  typed_embedded_schema do
    field :action, Ecto.Enum, values: [:index, :delete, :function]
    field :index_name, :string
    field :filter, :string
    field :function, :string
    field :context, :map
  end

  def changeset(struct, params) do
    allowed_keys = [:action, :index_name, :filter, :function, :context]

    struct
    |> cast(params, allowed_keys, empty_values: [])
    |> Routing.Helpers.validate_no_extra_keys(params, allowed_keys)
    |> validate_required([:action, :index_name])
    |> validate_inclusion(:action, [:index, :delete, :function])
    |> validate_length(:index_name, min: 1, max: 1024)
    |> validate_function_fields()
  end

  defp validate_function_fields(changeset) do
    action = get_field(changeset, :action)

    if action == :function do
      changeset
      |> validate_required([:filter, :function])
      |> validate_length(:filter, min: 1, max: 10_000)
      |> validate_length(:function, min: 1, max: 10_000)
    else
      changeset
    end
  end

  def route(action, _record, _changes, _metadata) do
    meilisearch_action =
      case action do
        "insert" -> "index"
        "update" -> "index"
        "delete" -> "delete"
        "read" -> "index"
      end

    %{action: meilisearch_action}
  end

  def route_consumer(%Sequin.Consumers.SinkConsumer{sink: sink}) do
    %{index_name: sink.index_name}
  end
end
