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

    test "validates SQS batch size cannot exceed 10" do
      # Test with batch size of 11 (should fail)
      attrs = ConsumersFactory.sink_consumer_attrs(type: :sqs, batch_size: 11)
      changeset = SinkConsumer.create_changeset(%SinkConsumer{}, attrs)

      refute changeset.valid?

      assert changeset.errors[:batch_size] ==
               {"SQS batch size cannot exceed 10 messages per batch",
                [validation: :number, kind: :less_than_or_equal_to, number: 10]}

      # Test with batch size of 10 (should pass)
      attrs = ConsumersFactory.sink_consumer_attrs(type: :sqs, batch_size: 10)
      changeset = SinkConsumer.create_changeset(%SinkConsumer{}, attrs)

      assert changeset.valid?

      # Test with batch size of 5 (should pass)
      attrs = ConsumersFactory.sink_consumer_attrs(type: :sqs, batch_size: 5)
      changeset = SinkConsumer.create_changeset(%SinkConsumer{}, attrs)

      assert changeset.valid?
    end
  end
end
