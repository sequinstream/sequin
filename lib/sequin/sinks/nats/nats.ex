defmodule Sequin.Sinks.Nats do
  @moduledoc false
  alias Sequin.Consumers.NatsSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error
  alias Sequin.Runtime.Routing.RoutedMessage

  @callback send_messages(SinkConsumer.t(), [RoutedMessage.t()]) ::
              :ok | {:error, Error.t()}
  @callback test_connection(NatsSink.t()) :: :ok | {:error, Error.t()}

  @client Application.compile_env(:sequin, :nats_module, Sequin.Sinks.Nats.Client)
  defdelegate send_messages(consumer, messages), to: @client

  defdelegate test_connection(sink), to: @client
end
