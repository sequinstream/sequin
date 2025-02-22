defmodule S2.V1alpha.BasinConfig do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.0", syntax: :proto3

  field :default_stream_config, 1, type: S2.V1alpha.StreamConfig, json_name: "defaultStreamConfig"
end
