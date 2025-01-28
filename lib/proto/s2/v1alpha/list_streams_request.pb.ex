defmodule S2.V1alpha.ListStreamsRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.0", syntax: :proto3

  field :prefix, 1, type: :string
  field :start_after, 2, type: :string, json_name: "startAfter"
  field :limit, 3, proto3_optional: true, type: :uint64
end
