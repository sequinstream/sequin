defmodule S2.V1alpha.BasinState do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.14.0", syntax: :proto3

  field :BASIN_STATE_UNSPECIFIED, 0
  field :BASIN_STATE_ACTIVE, 1
  field :BASIN_STATE_CREATING, 2
  field :BASIN_STATE_DELETING, 3
end
