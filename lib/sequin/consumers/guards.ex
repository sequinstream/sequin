defmodule Sequin.Consumers.Guards do
  @moduledoc """
  Provides guard macros for Sequin.Consumers types.
  """

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord

  @doc """
  Guard that checks if the given term is either a ConsumerEvent or a ConsumerRecord.
  """
  defguard is_event_or_record(term)
           when is_struct(term, ConsumerEvent) or is_struct(term, ConsumerRecord)
end
