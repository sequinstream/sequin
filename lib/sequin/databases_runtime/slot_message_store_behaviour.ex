defmodule Sequin.DatabasesRuntime.SlotMessageStoreBehaviour do
  @moduledoc """
  Behaviour for SlotMessageStore implementations.
  """
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord

  @type consumer_id :: String.t()
  @type ack_id :: String.t()

  @callback ack(consumer_id(), list(ack_id())) :: {:ok, non_neg_integer()} | {:error, Exception.t()}

  @callback produce(consumer_id(), pos_integer(), pid()) ::
              {:ok, list(ConsumerRecord.t() | ConsumerEvent.t())} | {:error, Exception.t()}
end
