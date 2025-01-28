defmodule S2.V1alpha.Header do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.0", syntax: :proto3

  field :name, 1, type: :bytes
  field :value, 2, type: :bytes
end
