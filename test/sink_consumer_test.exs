defmodule Sequin.SinkConsumerTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Factory.ConsumersFactory

  describe "sink consumer create_changeset/2" do
    test "all sinks have valid factories" do
      for sink_type <- Consumers.SinkConsumer.types() do
        attrs = ConsumersFactory.sink_consumer_attrs(type: sink_type)
        changeset = SinkConsumer.create_changeset(%SinkConsumer{}, attrs)
        errors = Sequin.Error.errors_on(changeset)
        assert changeset.valid?, "Sink #{sink_type} has invalid factory: #{inspect(errors)}"
      end
    end
  end
end
