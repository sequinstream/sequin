defmodule Sequin.Runtime.Routing.Consumers.Sns do
  @moduledoc false
  use Sequin.Runtime.Routing.RoutedConsumer

  alias Sequin.Consumers.SnsSink
  alias Sequin.Runtime.Routing

  @primary_key false
  @derive {Jason.Encoder, only: [:topic_arn]}
  typed_embedded_schema do
    field :topic_arn, :string
  end

  def changeset(struct, params) do
    allowed_keys = [:topic_arn]

    struct
    |> cast(params, allowed_keys, empty_values: [])
    |> Routing.Helpers.validate_no_extra_keys(params, allowed_keys)
    |> validate_required([:topic_arn])
    |> validate_length(:topic_arn, min: 1, max: 2000)
    |> validate_format(:topic_arn, SnsSink.sns_arn_regex(),
      message: "must be a valid AWS SNS Topic ARN (arn:aws:sns:<region>:<account-id>:<topic-name>)"
    )
  end

  def route(_action, _record, _changes, _metadata) do
    %{
      topic_arn: nil
    }
  end

  def route_consumer(%Sequin.Consumers.SinkConsumer{sink: sink}) do
    %{topic_arn: sink.topic_arn}
  end
end
