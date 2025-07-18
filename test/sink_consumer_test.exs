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
    setup do
      enrichment_function = FunctionsFactory.insert_enrichment_function!()
      transform_function = FunctionsFactory.insert_transform_function!()

      %{enrichment_function: enrichment_function, transform_function: transform_function}
    end

    test "can create a sink consumer with a sql enrichment function when source has a single table", %{
      enrichment_function: enrichment_function
    } do
      sink_consumer_attrs =
        ConsumersFactory.sink_consumer_attrs(
          enrichment_id: enrichment_function.id,
          source: %{include_table_oids: [1]}
        )

      changeset = SinkConsumer.create_changeset(%SinkConsumer{}, sink_consumer_attrs)
      assert changeset.valid?
    end

    test "cannot create a sink consumer with a sql enrichment function when source has multiple tables", %{
      enrichment_function: enrichment_function
    } do
      sink_consumer_attrs =
        ConsumersFactory.sink_consumer_attrs(
          enrichment_id: enrichment_function.id,
          source: %{include_table_oids: [1, 2]}
        )

      changeset = SinkConsumer.create_changeset(%SinkConsumer{}, sink_consumer_attrs)
      errors = Sequin.Error.errors_on(changeset)
      assert %{enrichment_id: [msg]} = errors
      assert msg =~ "Enrichment is not supported for multiple tables"
    end

    test "cannot create a sink consumer with sql enrichment when source is nil", %{
      enrichment_function: enrichment_function
    } do
      sink_consumer_attrs = ConsumersFactory.sink_consumer_attrs(enrichment_id: enrichment_function.id)
      changeset = SinkConsumer.create_changeset(%SinkConsumer{}, sink_consumer_attrs)
      errors = Sequin.Error.errors_on(changeset)
      assert %{enrichment_id: [msg]} = errors
      assert msg =~ "Enrichment is not supported for multiple tables"
    end

    test "cannot assign a non-enrichment function to enrichment", %{transform_function: transform_function} do
      sink_consumer_attrs =
        ConsumersFactory.sink_consumer_attrs(enrichment_id: transform_function.id, source: %{include_table_oids: [1]})

      changeset = SinkConsumer.create_changeset(%SinkConsumer{}, sink_consumer_attrs)
      assert_raise Postgrex.Error, fn -> Repo.insert(changeset) end
    end
  end
end
