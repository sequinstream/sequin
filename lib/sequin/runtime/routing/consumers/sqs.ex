defmodule Sequin.Runtime.Routing.Consumers.Sqs do
  @moduledoc false
  use Sequin.Runtime.Routing.RoutedConsumer

  alias Sequin.Consumers.SqsSink
  alias Sequin.Runtime.Routing

  @primary_key false
  @derive {Jason.Encoder, only: [:queue_url]}
  typed_embedded_schema do
    field :queue_url, :string
  end

  @impl true
  def changeset(struct, params) do
    allowed_keys = [:queue_url]

    struct
    |> cast(params, allowed_keys, empty_values: [])
    |> Routing.Helpers.validate_no_extra_keys(params, allowed_keys)
    |> validate_required([:queue_url])
    |> validate_length(:queue_url, max: 2000)
    |> validate_format(:queue_url, SqsSink.sqs_url_regex(),
      message: "must be a valid AWS SQS URL (https://sqs.<region>.amazonaws.com/<account-id>/<queue-name>)"
    )
  end

  @impl true
  def route(_action, _record, _changes, _metadata) do
    %{queue_url: nil}
  end

  @impl true
  def route_consumer(%Sequin.Consumers.SinkConsumer{sink: sink}) do
    %{queue_url: sink.queue_url}
  end
end
