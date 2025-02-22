defmodule S2.V1alpha.StreamConfig do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.0", syntax: :proto3

  oneof(:retention_policy, 0)

  field :storage_class, 1, type: S2.V1alpha.StorageClass, json_name: "storageClass", enum: true
  field :age, 2, type: :uint64, oneof: 0
end
