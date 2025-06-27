defmodule Sequin.Consumers.MeilisearchSinkTest do
  use ExUnit.Case, async: true

  alias Sequin.Consumers.MeilisearchSink

  describe "changeset/2" do
    setup do
      %{
        valid_params: %{
          endpoint_url: "https://meilisearch.com",
          index_name: "test",
          api_key: "test",
          routing_mode: :static
        }
      }
    end

    test "valid params have no errors", %{valid_params: params} do
      changeset = MeilisearchSink.changeset(%MeilisearchSink{}, params)
      assert Sequin.Error.errors_on(changeset) == %{}
    end

    test "validates endpoint_url with missing scheme", %{valid_params: params} do
      changeset = MeilisearchSink.changeset(%MeilisearchSink{}, %{params | endpoint_url: "invalid"})
      assert Sequin.Error.errors_on(changeset)[:endpoint_url] == ["must include a scheme, ie. https://"]
    end

    test "validates endpoint_url with invalid scheme", %{valid_params: params} do
      changeset = MeilisearchSink.changeset(%MeilisearchSink{}, %{params | endpoint_url: "ftp://meilisearch.com"})
      assert Sequin.Error.errors_on(changeset)[:endpoint_url] == ["must include a valid scheme, ie. http or https"]
    end

    test "validates endpoint_url with missing host", %{valid_params: params} do
      changeset = MeilisearchSink.changeset(%MeilisearchSink{}, %{params | endpoint_url: "https://"})
      assert Sequin.Error.errors_on(changeset)[:endpoint_url] == ["must include a host"]
    end

    test "validates endpoint_url with query params", %{valid_params: params} do
      changeset =
        MeilisearchSink.changeset(%MeilisearchSink{}, %{params | endpoint_url: "https://meilisearch.com?param=value"})

      assert Sequin.Error.errors_on(changeset)[:endpoint_url] == ["must not include query params, found: param=value"]
    end

    test "validates endpoint_url with fragment", %{valid_params: params} do
      changeset =
        MeilisearchSink.changeset(%MeilisearchSink{}, %{params | endpoint_url: "https://meilisearch.com#fragment"})

      assert Sequin.Error.errors_on(changeset)[:endpoint_url] == ["must not include a fragment, found: fragment"]
    end

    test "sets index_name to blank when routing_mode is dynamic", %{valid_params: params} do
      changeset =
        MeilisearchSink.changeset(%MeilisearchSink{}, %{params | routing_mode: :dynamic})

      refute :index_name in changeset.changes
    end
  end
end
