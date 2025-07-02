defmodule Sequin.Runtime.Routing.Consumers.AzureEventHub do
  @moduledoc false
  use Sequin.Runtime.Routing.RoutedConsumer

  @primary_key false
  @derive {Jason.Encoder, only: [:event_hub_name]}
  typed_embedded_schema do
    field :event_hub_name, :string
  end

  def changeset(struct, params) do
    allowed_keys = [:event_hub_name]

    struct
    |> cast(params, allowed_keys, empty_values: [])
    |> Routing.Helpers.validate_no_extra_keys(params, allowed_keys)
    |> validate_required([:event_hub_name])
    |> validate_length(:event_hub_name, min: 1, max: 255)
  end

  def route(_action, _record, _changes, _metadata) do
    %{event_hub_name: nil}
  end

  def route_consumer(%Sequin.Consumers.SinkConsumer{sink: sink}) do
    %{event_hub_name: sink.event_hub_name}
  end
end
