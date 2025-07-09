defmodule Sequin.Runtime.SlotProducer.Batch do
  @moduledoc false
  use TypedStruct

  alias Sequin.Replication
  alias Sequin.Runtime.SlotProducer.BatchMarker

  @derive {Inspect, except: [:messages]}
  typedstruct do
    field :high_watermark_wal_cursor, Replication.wal_cursor()
    field :epoch, BatchMarker.epoch()
    field :messages, list(), default: []
    field :markers_received, MapSet.t(), default: MapSet.new()
  end

  def init_from_marker(%BatchMarker{} = marker) do
    %__MODULE__{
      epoch: marker.epoch,
      high_watermark_wal_cursor: marker.high_watermark_wal_cursor,
      markers_received: MapSet.new([marker.processor_partition_idx])
    }
  end

  def put_marker(%__MODULE__{epoch: epoch} = batch, %BatchMarker{epoch: epoch} = marker) do
    high_watermark_cursor =
      cond do
        batch.high_watermark_wal_cursor == marker.high_watermark_wal_cursor ->
          batch.high_watermark_wal_cursor

        is_nil(batch.high_watermark_wal_cursor) and not is_nil(marker.high_watermark_wal_cursor) ->
          marker.high_watermark_wal_cursor

        true ->
          raise "Invariant error: high_watermark_cursor mismatch (batch: #{inspect(Map.delete(batch, :messages))}, marker: #{inspect(marker)})"
      end

    %__MODULE__{
      batch
      | high_watermark_wal_cursor: high_watermark_cursor,
        markers_received: MapSet.put(batch.markers_received, marker.processor_partition_idx)
    }
  end
end
