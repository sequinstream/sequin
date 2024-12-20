defmodule Sequin.Sinks.Nats do
  @moduledoc false
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.NatsSink
  alias Sequin.Error

  @callback send_messages(NatsSink.t(), [ConsumerRecord.t() | ConsumerEvent.t()]) ::
              :ok | {:error, Error.t()}
  @callback test_connection(NatsSink.t()) :: :ok | {:error, Error.t()}

  @client Application.compile_env(:sequin, :nats_module, Sequin.Sinks.Nats.Client)
  defdelegate send_messages(sink, messages), to: @client

  defdelegate test_connection(sink), to: @client
end
