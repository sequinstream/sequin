defmodule Sequin.Runtime.ConsumerMessageBatch do
  @moduledoc false
  use TypedStruct
  use TypedStruct

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Replication

  typedstruct do
    field :messages, [ConsumerRecord.t() | ConsumerEvent.t()]
    field :high_watermark_wal_cursor, Replication.wal_cursor()
  end
end
