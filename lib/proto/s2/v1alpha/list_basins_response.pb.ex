defmodule S2.V1alpha.ListBasinsResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.0", syntax: :proto3

  field :basins, 1, repeated: true, type: S2.V1alpha.BasinInfo
  field :has_more, 2, type: :bool, json_name: "hasMore"
end
