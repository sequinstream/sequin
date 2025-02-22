defmodule S2.V1alpha.CreateBasinRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.0", syntax: :proto3

  oneof(:assignment, 0)

  field :basin, 1, type: :string
  field :config, 2, type: S2.V1alpha.BasinConfig
  field :scope, 3, type: :string, oneof: 0
  field :cell, 4, type: :string, oneof: 0
end
