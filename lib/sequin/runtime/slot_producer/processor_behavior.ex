defmodule Sequin.Runtime.SlotProducer.ProcessorBehaviour do
  @moduledoc false
  alias Sequin.Runtime.SlotProducer.BatchMarker
  alias Sequin.Runtime.SlotProducer.Relation

  @callback handle_relation(processor :: GenServer.server(), relation :: Relation.t()) :: :ok
  @callback handle_batch_marker(processor :: GenServer.server(), batch :: BatchMarker.t()) :: :ok
end
