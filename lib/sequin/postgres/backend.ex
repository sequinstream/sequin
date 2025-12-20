defmodule Sequin.Postgres.Backend do
  @moduledoc """
  Behaviour for Postgres replication protocol backends.

  Allows SlotProducer to work with different backends:
  - PostgrexBackend: Real Postgrex.Protocol wrapper
  - FakegresBackend: Fake backend for benchmarking
  """

  @type state :: term()
  @type copies :: [binary()]

  @callback connect(opts :: keyword()) :: {:ok, state()} | {:error, term()}

  @callback handle_streaming(query :: String.t(), state()) ::
              {:ok, state()} | {:error, term(), state()}

  @callback checkin(state()) :: {:ok, state()} | {:error, term()}

  @callback handle_copy_recv(socket_msg :: term(), max :: pos_integer(), state()) ::
              {:ok, copies(), state()} | {:error | :disconnect, term(), state()}

  @callback handle_copy_send(msg :: iodata(), state()) ::
              :ok | {:error | :disconnect, term(), state()}

  @callback handle_simple(query :: String.t(), params :: list(), state()) ::
              {:ok, list(), state()} | {:error, term()}

  @callback disconnect(reason :: term(), state()) :: :ok
end
