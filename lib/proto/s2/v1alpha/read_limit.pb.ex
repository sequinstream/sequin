defmodule S2.V1alpha.ReadLimit do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.0", syntax: :proto3

  field :count, 1, proto3_optional: true, type: :uint64
  field :bytes, 2, proto3_optional: true, type: :uint64
end
