defmodule Sequin.Runtime.Routing.Consumers.RedisString do
  @moduledoc false
  use Sequin.Runtime.Routing.RoutedConsumer

  @primary_key false
  @derive {Jason.Encoder, only: [:key, :action, :expire_ms]}
  typed_embedded_schema do
    field :key, :string
    field :action, :string
    field :expire_ms, :integer
  end

  def changeset(struct, params) do
    allowed_keys = [:key, :action, :expire_ms]

    struct
    |> cast(Routing.Helpers.cast_numeric_to_string_fields(params, [:key]), allowed_keys, empty_values: [])
    |> Routing.Helpers.validate_no_extra_keys(params, allowed_keys)
    |> validate_required([:key, :action])
    |> validate_length(:key, min: 1, max: 512)
    |> validate_format(:key, ~r/^[^\s]+$/, message: "cannot contain whitespace")
    |> validate_inclusion(:action, ["set", "del"], message: "is invalid, valid values are: set, del")
    |> validate_number(:expire_ms, greater_than: 0)
  end

  def route(action, _record, _changes, metadata) do
    table_name = metadata.table_name
    pks = Enum.join(metadata.record_pks, "-")

    redis_action =
      case action do
        "insert" -> "set"
        "update" -> "set"
        "delete" -> "del"
        "read" -> "set"
      end

    %{key: "sequin:#{table_name}:#{pks}", action: redis_action, expire_ms: nil}
  end

  def route_consumer(%Sequin.Consumers.SinkConsumer{sink: sink}) do
    %{expire_ms: sink.expire_ms}
  end
end
