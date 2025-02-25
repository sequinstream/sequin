defmodule S2.V1alpha.ReadRequest do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.0", syntax: :proto3

  field :stream, 1, type: :string
  field :start_seq_num, 2, type: :uint64, json_name: "startSeqNum"
  field :limit, 3, type: S2.V1alpha.ReadLimit
end
