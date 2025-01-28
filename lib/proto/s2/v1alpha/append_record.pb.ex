defmodule S2.V1alpha.AppendRecord do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.0", syntax: :proto3

  field :headers, 1, repeated: true, type: S2.V1alpha.Header
  field :body, 2, type: :bytes
end
