defmodule Sequin.Runtime.SlotMessageStoreBehaviour do
  @moduledoc """
  Behaviour for SlotMessageStore implementations.
  """
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord

  @type consumer_id :: String.t()
  @type ack_id :: String.t()

  @callback messages_succeeded(consumer_id(), list(ack_id())) ::
              {:ok, non_neg_integer()} | {:error, Exception.t()}

  @callback messages_succeeded_returning_messages(consumer_id(), list(ack_id())) ::
              {:ok, [message :: map()]} | {:error, Exception.t()}

  @callback messages_already_succeeded(consumer_id(), list(ack_id())) ::
              {:ok, non_neg_integer()} | {:error, Exception.t()}

  @callback messages_failed(consumer_id(), list(ConsumerRecord.t() | ConsumerEvent.t())) :: :ok | {:error, Exception.t()}

  @callback produce(consumer_id(), pos_integer(), pid()) ::
              {:ok, list(ConsumerRecord.t() | ConsumerEvent.t())} | {:error, Exception.t()}
end
