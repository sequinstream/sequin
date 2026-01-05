defmodule Sequin.Consumers.Guards do
  @moduledoc """
  Provides guard macros for Sequin.Consumers types.
  """

  alias Sequin.Consumers.RedisStreamSink
  alias Sequin.Consumers.RedisStringSink

  defguard is_redis_sink(sink)
           when is_struct(sink, RedisStreamSink) or is_struct(sink, RedisStringSink)
end
