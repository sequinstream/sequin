defmodule Sequin.Nats do
  @moduledoc false
  alias Sequin.Consumers.ConsumerEventData
  alias Sequin.Consumers.ConsumerRecordData
  alias Sequin.Consumers.NatsSink
  alias Sequin.Error

  @callback send_messages(NatsSink.t(), [ConsumerRecordData.t() | ConsumerEventData.t()]) ::
              :ok | {:error, Error.t()}
  @callback test_connection(NatsSink.t()) :: :ok | {:error, Error.t()}

  @client Application.compile_env(:sequin, :nats_module, Sequin.Nats.Client)
  defdelegate send_messages(sink, messages), to: @client

  defdelegate test_connection(sink), to: @client
end
