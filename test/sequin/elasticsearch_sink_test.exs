defmodule Sequin.Consumers.ElasticsearchSinkTest do
  use ExUnit.Case, async: true

  alias Sequin.Consumers.ElasticsearchSink

  describe "changeset/2" do
    test "requires index_name when routing_mode is static" do
      params = %{
        endpoint_url: "https://es.example.com",
        index_name: "my-index",
        auth_type: :api_key,
        auth_value: "secret",
        routing_mode: :static
      }

      changeset = ElasticsearchSink.changeset(%ElasticsearchSink{}, params)
      assert changeset.valid?
    end

    test "clears index_name when routing_mode is dynamic" do
      params = %{
        endpoint_url: "https://es.example.com",
        index_name: "my-index",
        auth_type: :api_key,
        auth_value: "secret",
        routing_mode: :dynamic
      }

      changeset = ElasticsearchSink.changeset(%ElasticsearchSink{}, params)
      refute Map.has_key?(changeset.changes, :index_name)
    end
  end
end
