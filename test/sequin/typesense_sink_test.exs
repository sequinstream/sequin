defmodule Sequin.Consumers.TypesenseSinkTest do
  use ExUnit.Case, async: true

  alias Sequin.Consumers.TypesenseSink

  describe "changeset/2" do
    setup do
      %{
        valid_params: %{
          endpoint_url: "https://typesense.com",
          collection_name: "test",
          api_key: "test",
          routing_mode: :static
        }
      }
    end

    test "valid params have no errors", %{valid_params: params} do
      changeset = TypesenseSink.changeset(%TypesenseSink{}, params)
      assert Sequin.Error.errors_on(changeset) == %{}
    end

    test "validates endpoint_url with missing scheme", %{valid_params: params} do
      changeset = TypesenseSink.changeset(%TypesenseSink{}, %{params | endpoint_url: "invalid"})
      assert Sequin.Error.errors_on(changeset)[:endpoint_url] == ["must include a scheme, ie. https://"]
    end

    test "validates endpoint_url with invalid scheme", %{valid_params: params} do
      changeset = TypesenseSink.changeset(%TypesenseSink{}, %{params | endpoint_url: "ftp://typesense.com"})
      assert Sequin.Error.errors_on(changeset)[:endpoint_url] == ["must include a valid scheme, ie. http or https"]
    end

    test "validates endpoint_url with missing host", %{valid_params: params} do
      changeset = TypesenseSink.changeset(%TypesenseSink{}, %{params | endpoint_url: "https://"})
      assert Sequin.Error.errors_on(changeset)[:endpoint_url] == ["must include a host"]
    end

    test "validates endpoint_url with query params", %{valid_params: params} do
      changeset = TypesenseSink.changeset(%TypesenseSink{}, %{params | endpoint_url: "https://typesense.com?param=value"})
      assert Sequin.Error.errors_on(changeset)[:endpoint_url] == ["must not include query params, found: param=value"]
    end

    test "validates endpoint_url with fragment", %{valid_params: params} do
      changeset = TypesenseSink.changeset(%TypesenseSink{}, %{params | endpoint_url: "https://typesense.com#fragment"})
      assert Sequin.Error.errors_on(changeset)[:endpoint_url] == ["must not include a fragment, found: fragment"]
    end

    test "sets collection_name to blank when routing_mode is dynamic", %{valid_params: params} do
      changeset =
        TypesenseSink.changeset(%TypesenseSink{}, %{params | routing_mode: :dynamic})

      refute Map.has_key?(changeset.changes, :collection_name)
    end

    test "requires routing_mode" do
      changeset =
        TypesenseSink.changeset(%TypesenseSink{}, %{endpoint_url: "https://example.com", api_key: "k"})

      assert Sequin.Error.errors_on(changeset)[:routing_mode] == ["is required"]
    end
  end
end
