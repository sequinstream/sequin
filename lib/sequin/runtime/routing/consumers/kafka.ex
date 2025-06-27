defmodule Sequin.Runtime.Routing.Consumers.Kafka do
  @moduledoc false
  use Sequin.Runtime.Routing.RoutedConsumer

  @primary_key false
  @derive {Jason.Encoder, only: [:topic]}
  typed_embedded_schema do
    field :topic, :string
  end

  def changeset(struct, params) do
    allowed_keys = [:topic]

    struct
    |> cast(params, allowed_keys, empty_values: [])
    |> Routing.Helpers.validate_no_extra_keys(params, allowed_keys)
    |> validate_required([:topic])
    |> validate_length(:topic, min: 1, max: 512)
    |> validate_format(:topic, ~r/^[^\s]+$/, message: "cannot contain whitespace")
  end

  def route(_action, _record, _changes, metadata) do
    %{topic: "sequin.#{metadata.database.name}.#{metadata.table_schema}.#{metadata.table_name}"}
  end

  def route_consumer(%Sequin.Consumers.SinkConsumer{sink: sink}) do
    %{topic: sink.topic}
  end
end
