defmodule S2.V1alpha.SequencedRecordBatch do
  @moduledoc false

  use Protobuf, protoc_gen_elixir_version: "0.14.0", syntax: :proto3

  field :records, 1, repeated: true, type: S2.V1alpha.SequencedRecord
end
