defmodule S2.V1alpha.AppendResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.0", syntax: :proto3

  field :output, 1, type: S2.V1alpha.AppendOutput
end
