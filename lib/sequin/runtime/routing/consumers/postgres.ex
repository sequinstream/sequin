defmodule Sequin.Runtime.Routing.Consumers.Postgres do
  @moduledoc false
  use Sequin.Runtime.Routing.RoutedConsumer

  @primary_key false
  @derive {Jason.Encoder, only: [:table_name]}
  typed_embedded_schema do
    field :table_name, :string
  end

  def changeset(struct, params) do
    allowed_keys = [:table_name]

    struct
    |> cast(params, allowed_keys, empty_values: [])
    |> Routing.Helpers.validate_no_extra_keys(params, allowed_keys)
    |> validate_required([:table_name])
    |> validate_length(:table_name, min: 1, max: 512)
    |> validate_format(:table_name, ~r/^[^\s]+$/, message: "cannot contain whitespace")
  end

  def route(_action, _record, _changes, metadata) do
    %{table_name: metadata.table_name}
  end

  def route_consumer(%Sequin.Consumers.SinkConsumer{sink: sink}) do
    %{table_name: sink.table_name}
  end
end
