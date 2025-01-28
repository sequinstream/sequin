defmodule Sequin.Sinks.S2 do
  @moduledoc false
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.S2Sink
  alias Sequin.Error

  @callback send_messages(S2Sink.t(), [ConsumerRecord.t() | ConsumerEvent.t()]) ::
              :ok | {:error, Error.t()}
  @callback test_connection(S2Sink.t()) :: :ok | {:error, Error.t()}

  @client Application.compile_env(:sequin, :s2_module, Sequin.Sinks.S2.Client)
  defdelegate send_messages(sink, messages), to: @client

  defdelegate test_connection(sink), to: @client
end
