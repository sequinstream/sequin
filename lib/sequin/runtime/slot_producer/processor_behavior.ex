defmodule Sequin.Runtime.SlotProducer.ProcessorBehaviour do
  @moduledoc false
  alias Sequin.Runtime.SlotProducer.Relation

  @callback handle_relation(processor :: GenServer.server(), relation :: Relation.t()) :: :ok
end
