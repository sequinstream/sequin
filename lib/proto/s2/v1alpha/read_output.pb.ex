defmodule S2.V1alpha.ReadOutput do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.0", syntax: :proto3

  oneof(:output, 0)

  field :batch, 1, type: S2.V1alpha.SequencedRecordBatch, oneof: 0
  field :first_seq_num, 2, type: :uint64, json_name: "firstSeqNum", oneof: 0
  field :next_seq_num, 3, type: :uint64, json_name: "nextSeqNum", oneof: 0
end
