defmodule Sequin.Runtime.Routing.Consumers.Kinesis do
  @moduledoc false
  use Sequin.Runtime.Routing.RoutedConsumer

  alias Sequin.Consumers.KinesisSink
  alias Sequin.Runtime.Routing

  @primary_key false
  @derive {Jason.Encoder, only: [:stream_arn]}
  typed_embedded_schema do
    field :stream_arn, :string
  end

  def changeset(struct, params) do
    allowed_keys = [:stream_arn]

    struct
    |> cast(params, allowed_keys, empty_values: [])
    |> Routing.Helpers.validate_no_extra_keys(params, allowed_keys)
    |> validate_required([:stream_arn])
    |> validate_length(:stream_arn, min: 1, max: 2000)
    |> validate_format(:stream_arn, KinesisSink.kinesis_arn_regex(),
      message: "must be a valid AWS Kinesis Stream ARN (arn:aws:kinesis:<region>:<account-id>:stream/<stream-name>)"
    )
  end

  def route(_action, _record, _changes, _metadata) do
    %{
      stream_arn: nil
    }
  end

  def route_consumer(%Sequin.Consumers.SinkConsumer{sink: sink}) do
    %{stream_arn: sink.stream_arn}
  end
end
