defmodule Sequin.Consumers.RabbitMqSinkTest do
  use ExUnit.Case, async: true

  alias Sequin.Consumers.RabbitMqSink

  describe "changeset/2" do
    test "requires exchange when routing_mode is static" do
      params = %{
        host: "localhost",
        port: 5672,
        exchange: "sequin",
        routing_mode: :static
      }

      changeset = RabbitMqSink.changeset(%RabbitMqSink{}, params)
      assert changeset.valid?
    end

    test "clears exchange when routing_mode is dynamic" do
      params = %{
        host: "localhost",
        port: 5672,
        exchange: "sequin",
        routing_mode: :dynamic
      }

      changeset = RabbitMqSink.changeset(%RabbitMqSink{}, params)
      refute Map.has_key?(changeset.changes, :exchange)
    end
  end
end
