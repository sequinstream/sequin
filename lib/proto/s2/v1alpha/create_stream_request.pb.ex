defmodule S2.V1alpha.CreateStreamRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.0", syntax: :proto3

  field :stream, 1, type: :string
  field :config, 2, type: S2.V1alpha.StreamConfig
end
