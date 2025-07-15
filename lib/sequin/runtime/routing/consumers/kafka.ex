defmodule Sequin.Runtime.Routing.Consumers.Kafka do
  @moduledoc false
  use Sequin.Runtime.Routing.RoutedConsumer

  @primary_key false
  @derive {Jason.Encoder, only: [:topic, :message_key]}
  typed_embedded_schema do
    field :topic, :string

    # Defaults to the `group_id` which is not available in def route/4
    field :message_key, :string
  end

  def changeset(struct, params) do
    allowed_keys = [:topic, :message_key]

    struct
    |> cast(params, allowed_keys, empty_values: [])
    |> Routing.Helpers.validate_no_extra_keys(params, allowed_keys)
    |> validate_required([:topic])
    |> validate_length(:topic, min: 1, max: 512)
    |> validate_format(:topic, ~r/^[^\s]+$/, message: "cannot contain whitespace")
    |> validate_length(:message_key, min: 1, max: 512)
    |> validate_format(:message_key, ~r/^[^\s]+$/, message: "cannot contain whitespace")
  end

  def route(_action, _record, _changes, metadata) do
    %{topic: "sequin.#{metadata.table_schema}.#{metadata.table_name}"}
  end

  def route_consumer(%Sequin.Consumers.SinkConsumer{sink: sink}) do
    %{topic: sink.topic}
  end
end
