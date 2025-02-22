defmodule S2.V1alpha.AppendOutput do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.0", syntax: :proto3

  field :start_seq_num, 1, type: :uint64, json_name: "startSeqNum"
  field :end_seq_num, 2, type: :uint64, json_name: "endSeqNum"
  field :next_seq_num, 3, type: :uint64, json_name: "nextSeqNum"
end
