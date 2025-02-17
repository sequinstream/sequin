defmodule Sequin.Constants do
  @moduledoc false

  def backfill_batch_low_watermark, do: "sequin.backfill-batch.low-watermark"
  def backfill_batch_high_watermark, do: "sequin.backfill-batch.high-watermark"
  def backfill_batch_watermark, do: "sequin.backfill-batch.watermark"
end
