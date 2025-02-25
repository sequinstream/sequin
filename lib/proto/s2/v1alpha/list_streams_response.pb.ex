defmodule S2.V1alpha.ListStreamsResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.0", syntax: :proto3

  field :streams, 1, repeated: true, type: S2.V1alpha.StreamInfo
  field :has_more, 2, type: :bool, json_name: "hasMore"
end
