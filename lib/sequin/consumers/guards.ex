defmodule Sequin.Consumers.Guards do
  @moduledoc """
  Provides guard macros for Sequin.Consumers types.
  """

  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.RedisStreamSink
  alias Sequin.Consumers.RedisStringSink

  @doc """
  Guard that checks if the given term is either a ConsumerEvent or a ConsumerRecord.
  """
  defguard is_event_or_record(term)
           when is_struct(term, ConsumerEvent) or is_struct(term, ConsumerRecord)

  defguard is_redis_sink(sink)
           when is_struct(sink, RedisStreamSink) or is_struct(sink, RedisStringSink)
end
