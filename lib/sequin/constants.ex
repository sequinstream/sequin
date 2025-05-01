defmodule Sequin.Constants do
  @moduledoc false

  def backfill_batch_high_watermark, do: "sequin.backfill-batch.high-watermark"

  def logical_messages_table_name, do: "sequin_logical_messages"
end
