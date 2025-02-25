defmodule S2.V1alpha.AppendInput do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.0", syntax: :proto3

  field :stream, 1, type: :string
  field :records, 2, repeated: true, type: S2.V1alpha.AppendRecord
  field :match_seq_num, 3, proto3_optional: true, type: :uint64, json_name: "matchSeqNum"
  field :fencing_token, 4, proto3_optional: true, type: :bytes, json_name: "fencingToken"
end
