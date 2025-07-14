defmodule Sequin.SinkConsumerTest do
  use Sequin.DataCase, async: true

  alias Sequin.Consumers
  alias Sequin.Consumers.SinkConsumer
  alias Sequin.Factory.ConsumersFactory
  alias Sequin.Factory.FunctionsFactory

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

  describe "functions" do
    test "can create a sink consumer with a sql enrichment function" do
      sql_enrichment_function = FunctionsFactory.insert_sql_enrichment_function!()
      sink_consumer_attrs = ConsumersFactory.sink_consumer_attrs(enrichment_id: sql_enrichment_function.id)
      changeset = SinkConsumer.create_changeset(%SinkConsumer{}, sink_consumer_attrs)
      assert changeset.valid?
    end

    test "cannot assign a non-enrichment function to enrichment" do
      function = FunctionsFactory.insert_function!(function_type: :transform)
      sink_consumer_attrs = ConsumersFactory.sink_consumer_attrs(enrichment_id: function.id)
      changeset = SinkConsumer.create_changeset(%SinkConsumer{}, sink_consumer_attrs)
      assert_raise Postgrex.Error, fn -> Repo.insert(changeset) end
    end
  end
end
