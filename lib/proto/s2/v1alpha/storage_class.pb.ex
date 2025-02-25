defmodule S2.V1alpha.StorageClass do
  @moduledoc false

  use Protobuf, enum: true, protoc_gen_elixir_version: "0.14.0", syntax: :proto3

  field :STORAGE_CLASS_UNSPECIFIED, 0
  field :STORAGE_CLASS_STANDARD, 1
  field :STORAGE_CLASS_EXPRESS, 2
end
