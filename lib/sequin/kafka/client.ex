defmodule Sequin.Kafka.Client do
  @moduledoc false
  @behaviour Sequin.Kafka

  alias Sequin.Consumers.KafkaDestination

  @impl Sequin.Kafka
  def publish(%KafkaDestination{} = _destination, _messages) do
    :ok
  end

  @impl Sequin.Kafka
  def test_connection(%KafkaDestination{} = _destination) do
    :ok
  end
end
