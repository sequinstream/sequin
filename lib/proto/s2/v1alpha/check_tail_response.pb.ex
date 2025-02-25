defmodule S2.V1alpha.CheckTailResponse do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.0", syntax: :proto3

  field :next_seq_num, 1, type: :uint64, json_name: "nextSeqNum"
end
