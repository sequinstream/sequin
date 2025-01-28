defmodule S2.V1alpha.BasinInfo do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.0", syntax: :proto3

  field :name, 1, type: :string
  field :scope, 2, type: :string
  field :cell, 3, type: :string
  field :state, 4, type: S2.V1alpha.BasinState, enum: true
end
