defmodule S2.V1alpha.SequencedRecord do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.0", syntax: :proto3

  field :seq_num, 1, type: :uint64, json_name: "seqNum"
  field :headers, 2, repeated: true, type: S2.V1alpha.Header
  field :body, 3, type: :bytes
end
