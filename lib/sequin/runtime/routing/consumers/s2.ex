defmodule Sequin.Runtime.Routing.Consumers.S2 do
  @moduledoc false
  use Sequin.Runtime.Routing.RoutedConsumer

  @primary_key false
  @derive {Jason.Encoder, only: [:basin, :stream]}
  typed_embedded_schema do
    field :basin, :string
    field :stream, :string
  end

  def changeset(struct, params) do
    allowed_keys = [:basin, :stream]

    struct
    |> cast(params, allowed_keys, empty_values: [])
    |> Routing.Helpers.validate_no_extra_keys(params, allowed_keys)
    |> validate_required([:basin, :stream])
    |> validate_length(:basin, min: 1, max: 48)
    |> validate_length(:stream, min: 1, max: 255)
  end

  def route(_action, _record, _changes, _metadata) do
    %{basin: nil, stream: nil}
  end

  def route_consumer(%Sequin.Consumers.SinkConsumer{sink: sink}) do
    %{basin: sink.basin, stream: sink.stream}
  end
end
