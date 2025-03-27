defmodule Sequin.Sinks.RabbitMq do
  @moduledoc false
  alias Sequin.Consumers.ConsumerEvent
  alias Sequin.Consumers.ConsumerRecord
  alias Sequin.Consumers.RabbitMqSink
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Error

  @callback send_messages(SinkConsumer.t(), [ConsumerRecord.t() | ConsumerEvent.t()]) ::
              :ok | {:error, Error.t()}
  @callback test_connection(RabbitMqSink.t()) :: :ok | {:error, Error.t()}

  @client Application.compile_env(:sequin, :rabbitmq_module, Sequin.Sinks.RabbitMq.Client)
  defdelegate send_messages(consumer, messages), to: @client

  defdelegate test_connection(sink), to: @client
end
