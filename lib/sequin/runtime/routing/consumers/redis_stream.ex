defmodule Sequin.Runtime.Routing.Consumers.RedisStream do
  @moduledoc false
  use Sequin.Runtime.Routing.RoutedConsumer

  @primary_key false
  @derive {Jason.Encoder, only: [:stream_key]}
  typed_embedded_schema do
    field :stream_key, :string
  end

  def changeset(struct, params) do
    allowed_keys = [:stream_key]

    struct
    |> cast(params, allowed_keys, empty_values: [])
    |> Routing.Helpers.validate_no_extra_keys(params, allowed_keys)
    |> validate_required([:stream_key])
    |> validate_length(:stream_key, min: 1, max: 255)
    |> validate_format(:stream_key, ~r/^\S+$/, message: "cannot contain whitespace")
  end

  def route(_action, _record, _changes, metadata) do
    %{stream_key: "sequin.#{metadata.table_schema}.#{metadata.table_name}"}
  end

  def route_consumer(%Sequin.Consumers.SinkConsumer{sink: sink}) do
    %{stream_key: sink.stream_key}
  end
end
