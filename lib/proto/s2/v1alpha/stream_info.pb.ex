defmodule S2.V1alpha.StreamInfo do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.0", syntax: :proto3

  field :name, 1, type: :string
  field :created_at, 2, type: :uint32, json_name: "createdAt"
  field :deleted_at, 3, proto3_optional: true, type: :uint32, json_name: "deletedAt"
end
