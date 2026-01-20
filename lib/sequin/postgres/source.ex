defmodule Sequin.Postgres.Source do
  @moduledoc """
  Behaviour for virtual WAL sources used with `VirtualBackend`.

  Implementations generate or provide WAL messages that flow through
  the replication pipeline without a real Postgres connection.

  ## Implementations

  - `Sequin.Postgres.BenchmarkSource` - Generates endless synthetic WAL
    messages for benchmarking throughput
  - `Sequin.Postgres.MockSource` (future) - Controllable source for tests
    where you can push specific messages
  """

  @type id :: term()

  @doc """
  Registers a producer to receive `:tcp` messages from this source.

  The source should send `{:tcp, source_name, :data_ready}` to the producer
  when data is available.
  """
  @callback set_producer(id(), producer :: pid()) :: :ok

  @doc """
  Receives up to `max_count` WAL copy messages.

  Returns a list of binary WAL messages. After returning, the source
  should send another `:tcp` message to keep the producer saturated.
  """
  @callback recv_copies(id(), max_count :: pos_integer()) :: [binary()]

  @doc """
  Handles an ack message from the replication protocol.
  """
  @callback handle_ack(id(), msg :: iodata()) :: :ok
end
