defmodule S2.V1alpha.CreateStreamResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.0", syntax: :proto3

  field :info, 1, type: S2.V1alpha.StreamInfo
end
