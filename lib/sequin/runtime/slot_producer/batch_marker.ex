defmodule Sequin.Runtime.SlotProducer.BatchMarker do
  @moduledoc false
  use TypedStruct

  alias Sequin.Replication

  @type epoch :: non_neg_integer()

  typedstruct do
    field :high_watermark_wal_cursor, Replication.wal_cursor(), enforce: true
    field :epoch, epoch(), enforce: true
    field :processor_partition_idx, non_neg_integer()
  end
end
