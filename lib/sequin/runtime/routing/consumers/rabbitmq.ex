defmodule Sequin.Runtime.Routing.Consumers.Rabbitmq do
  @moduledoc false
  use Sequin.Runtime.Routing.RoutedConsumer

  @primary_key false
  @derive {Jason.Encoder, only: [:exchange, :headers]}
  typed_embedded_schema do
    field :exchange, :string
    field :routing_key, :string
    field :message_id, :string
    field :headers, :map
  end

  def changeset(struct, params) do
    allowed_keys = [:exchange, :headers, :routing_key, :message_id]

    struct
    |> cast(params, allowed_keys, empty_values: [])
    |> Routing.Helpers.validate_no_extra_keys(params, allowed_keys)
    |> validate_required([:exchange])
    |> validate_length(:exchange, max: 255)
  end

  def route(action, _record, _changes, metadata) do
    routing_key = "sequin.#{metadata.database_name}.#{metadata.table_schema}.#{metadata.table_name}.#{action}"

    %{
      exchange: nil,
      headers: %{},
      routing_key: routing_key,
      message_id: metadata.idempotency_key
    }
  end

  def route_consumer(%Sequin.Consumers.SinkConsumer{sink: sink}) do
    %{exchange: sink.exchange, headers: sink.headers || %{}}
  end
end
