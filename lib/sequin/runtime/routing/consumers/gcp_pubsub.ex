defmodule Sequin.Runtime.Routing.Consumers.GcpPubsub do
  @moduledoc false
  use Sequin.Runtime.Routing.RoutedConsumer

  @primary_key false
  @derive {Jason.Encoder, only: [:topic_id]}
  typed_embedded_schema do
    field :topic_id, :string
  end

  def changeset(struct, params) do
    allowed_keys = [:topic_id]

    struct
    |> cast(params, allowed_keys, empty_values: [])
    |> Routing.Helpers.validate_no_extra_keys(params, allowed_keys)
    |> validate_required([:topic_id])
    |> validate_length(:topic_id, min: 1, max: 255)
    |> validate_format(:topic_id, ~r/^[a-zA-Z][a-zA-Z0-9-_.~+%]{2,254}$/,
      message: "must be between 3 and 255 characters and match the pattern: [a-zA-Z][a-zA-Z0-9-_.~+%]*"
    )
  end

  def route(_action, _record, _changes, metadata) do
    %{
      topic_id: "sequin.#{metadata.database.name}.#{metadata.table_schema}.#{metadata.table_name}"
    }
  end

  def route_consumer(%Sequin.Consumers.SinkConsumer{sink: sink}) do
    %{topic_id: sink.topic_id}
  end
end
