defmodule Sequin.Runtime.SlotProducer.BatchMarker do
  @moduledoc false
  use TypedStruct

  alias Sequin.Replication

  @type idx :: non_neg_integer()

  typedstruct do
    field :high_watermark_wal_cursor, Replication.wal_cursor(), enforce: true
    field :idx, idx(), enforce: true
    field :producer_partition_idx, non_neg_integer()
  end
end
